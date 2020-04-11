/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.memory

import java.nio.ByteBuffer
import java.util.Collections
import java.util.function.BiConsumer

import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.cache.LowMemoryException
import com.gemstone.gemfire.distributed.DistributedMember
import com.gemstone.gemfire.internal.i18n.LocalizedStrings
import com.gemstone.gemfire.internal.shared.BufferAllocator
import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator.{DIRECT_OBJECT_OVERHEAD, DIRECT_STORE_OBJECT_OWNER}
import com.gemstone.gemfire.internal.shared.unsafe.{DirectBufferAllocator, FreeMemory, UnsafeHolder}
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.columnar.impl.{StoreCallbacksImpl => callbacks}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryAllocator

/**
 * Extension to [[DirectBufferAllocator]] that integrates with `SnappyUnifiedMemoryManager`.
 */
class UMMBufferAllocator extends DirectBufferAllocator with Logging {

  private val freeStorageMemoryFactory = new FreeMemory.Factory {
    override def newFreeMemory(address: Long, size: Int): FreeMemory = {
      new UMMFreeStorageMemory(address, size)
    }
  }

  private def newFreeMemoryFactory(owner: String): FreeMemory.Factory = new FreeMemory.Factory {
    override def newFreeMemory(address: Long, size: Int): FreeMemory = {
      new UMMFreeMemory(address, size, owner)
    }
  }

  DirectBufferAllocator.setInstance(this)

  override def allocate(size: Int, owner: String): ByteBuffer =
    allocate(owner, size, newFreeMemoryFactory(owner))

  override def allocateForStorage(size: Int): ByteBuffer =
    allocate(DIRECT_STORE_OBJECT_OWNER, size, freeStorageMemoryFactory)

  /**
   * Try to acquire off-heap memory from storage area.
   */
  private def tryAcquireStorageMemory(owner: String, numBytes: Long): Boolean = {
    // first try allocation without eviction
    if (callbacks.acquireStorageMemory(owner, numBytes, buffer = null,
      shouldEvict = false, offHeap = true)) {
      return true
    }

    // next are the tries with eviction
    var maxWaitMillis = 10000L
    val waitMillis = 1000L
    while (maxWaitMillis > 0L) {
      UnsafeHolder.releasePendingReferences()
      if (callbacks.waitForRuntimeManager(waitMillis)) maxWaitMillis = 0L
      else maxWaitMillis -= waitMillis

      if (callbacks.acquireStorageMemory(owner, numBytes, buffer = null,
        shouldEvict = true, offHeap = true)) {
        return true
      }
    }
    false
  }

  private def handleAllocateFailure(t: Throwable, owner: String, size: Int): Throwable = t match {
    case err: Error if SystemFailure.isJVMFailureError(err) =>
      SystemFailure.initiateFailure(err)
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      err
    case _ =>
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure()
      // release the acquired delta before failing
      callbacks.releaseStorageMemory(owner, size, offHeap = true)
      t
  }

  private def allocate(owner: String, size: Int, factory: FreeMemory.Factory): ByteBuffer = {
    val totalSize = UnsafeHolder.getAllocationSize(size) + DIRECT_OBJECT_OVERHEAD
    if (tryAcquireStorageMemory(owner, totalSize)) {
      try {
        val buffer = UnsafeHolder.allocateDirectBuffer(size, factory)
        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
          fill(buffer, MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE)
        }
        buffer
      } catch {
        case t: Throwable => throw handleAllocateFailure(t, owner, totalSize)
      }
    } else throw lowMemoryException("allocate", size)
  }

  override def isManagedDirect: Boolean = true

  override def lowMemoryException(op: String, required: Int): RuntimeException = {
    val id: DistributedMember = GemFireStore.getMyId
    val m = if (id ne null) Collections.singleton(id) else Collections.emptySet[DistributedMember]
    val ex = new LowMemoryException(LocalizedStrings
        .ResourceManager_LOW_MEMORY_FOR_0_FUNCEXEC_MEMBERS_1.toLocalizedString(
      s"UMMBufferAllocator.$op (maxStorage=${callbacks.getStoragePoolSize(true)} " +
          s"used=${callbacks.getStoragePoolUsedMemory(true)} required=$required)", m), m)
    logWarning(ex.toString)
    ex
  }

  override def expand(buffer: ByteBuffer, required: Int, owner: String): ByteBuffer = {
    assert(required > 0, s"UMMBufferAllocator.expand: required = $required")

    val used = buffer.limit()
    val capacity = buffer.capacity()
    if (used + required > capacity) {
      val newLength = BufferAllocator.expandedSize(used, required)
      val delta = UnsafeHolder.getAllocationSize(newLength) - capacity
      if (tryAcquireStorageMemory(owner, delta)) {
        try {
          val newBuffer = if (owner == DIRECT_STORE_OBJECT_OWNER) {
            UnsafeHolder.reallocateDirectBuffer(buffer, newLength,
              classOf[UMMFreeStorageMemory], freeStorageMemoryFactory)
          } else {
            UnsafeHolder.reallocateDirectBuffer(buffer, newLength,
              classOf[UMMFreeMemory], newFreeMemoryFactory(owner))
          }
          if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            // fill the delta bytes but don't touch the position in the buffer to be returned
            val cleanBuffer = newBuffer.duplicate()
            cleanBuffer.position(used)
            fill(cleanBuffer, MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE)
          }
          newBuffer
        } catch {
          case t: Throwable => throw handleAllocateFailure(t, owner, delta)
        }
      } else throw lowMemoryException("expand", delta)
    } else {
      // space already available in the buffer so return the same with increased limit
      buffer.limit(used + required)
      buffer
    }
  }

  override def changeOwnerToStorage(buffer: ByteBuffer, capacity: Int,
      changeOwner: BiConsumer[String, AnyRef]): Unit = {
    try {
      UnsafeHolder.changeDirectBufferCleaner(buffer, capacity, classOf[UMMFreeMemory],
        classOf[UMMFreeStorageMemory], freeStorageMemoryFactory, changeOwner)
    } catch {
      case e: Exception => throw new IllegalStateException(
        s"UMMBufferAllocator.changeOwnerToStorage: failed for $buffer", e)
    }
  }

  override def close(): Unit = {
    try {
      var allocated = 0L
      var tries = 4
      while (tries > 0) {
        allocated = callbacks.getOffHeapMemory(DIRECT_STORE_OBJECT_OWNER)
        if (allocated == 0L) return
        UnsafeHolder.releasePendingReferences()
        Thread.sleep(10L)
        tries -= 1
      }
      if (allocated > 0L) logInfo(s"Unreleased memory $allocated bytes in close()")
    } finally {
      DirectBufferAllocator.resetInstance()
    }
  }
}

/**
 * Free off-heap memory while also informing SnappyUMM.
 */
class UMMFreeMemory(address: Long, size: Int,
    override protected val owner: String) extends FreeMemory(address) with Logging {

  override final def run(): Unit = {
    val address = getAndResetAddress()
    if (address != 0L) {
      Platform.freeMemory(address)
      try {
        // release memory from SnappyUMM
        callbacks.releaseStorageMemory(owner, size + DIRECT_OBJECT_OVERHEAD, offHeap = true)
      } catch {
        case t: Throwable =>
          // check for system failure
          SystemFailure.checkFailure()
          try {
            logError("UMMFreeMemory: unexpected exception", t)
          } catch {
            case _: Throwable => // ignore at this point
          }
      }
    }
  }
}

/**
 * Need a specific class for `DIRECT_STORE_OBJECT_OWNER` since `changeOwnerToStorage` compares
 * by class to confirm if change of ownership is really required.
 */
class UMMFreeStorageMemory(address: Long, size: Int)
    extends UMMFreeMemory(address, size, DIRECT_STORE_OBJECT_OWNER)
