/**
 * @module
 * Stream utility.
 */

const responseQueueAborts = new WeakMap<ReadableStream, () => void>()

/** @internal */
export function createStreamingApi<T extends StreamingApi>(
  Api: new (writable: WritableStream, readable: ReadableStream) => T
): T {
  let readableController!: ReadableStreamDefaultController<Uint8Array>
  let writableController!: WritableStreamDefaultController
  let resumeWrite: (() => void) | undefined
  let open = true

  function cancelQueue(closeReadable: boolean) {
    if (!open) {
      return
    }
    open = false
    if (closeReadable) {
      readableController.close()
    }
    writableController.error()
    resumeWrite?.()
  }

  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      readableController = controller
    },
    pull() {
      resumeWrite?.()
    },
    cancel() {
      cancelQueue(false)
      if (!api.closed) {
        api.abort()
      }
    },
  })
  const writable = new WritableStream<Uint8Array>({
    start(controller) {
      writableController = controller
    },
    write(chunk) {
      if (!open) {
        return
      }
      if ((readableController.desiredSize ?? 0) > 0) {
        readableController.enqueue(chunk)
        return
      }
      // The writable runs one write at a time. A read frees space for it.
      return new Promise<void>((resolve) => {
        resumeWrite = () => {
          resumeWrite = undefined
          if (open) {
            readableController.enqueue(chunk)
          }
          resolve()
        }
      })
    },
    close() {
      if (open) {
        open = false
        readableController.close()
      }
    },
  })

  // Internal helpers already own the response queue. Keep the constructor's
  // forwarding behavior for callers that pass their own streams.
  responseQueueAborts.set(readable, () => cancelQueue(true))
  const api = new Api(writable, readable)
  return api
}

export class StreamingApi {
  private writer: WritableStreamDefaultWriter<Uint8Array>
  private encoder: TextEncoder
  private writable: WritableStream
  private abortSubscribers: (() => void | Promise<void>)[] = []
  responseReadable: ReadableStream
  /**
   * Whether the stream has been aborted.
   */
  aborted: boolean = false
  /**
   * Whether the stream has been closed normally.
   */
  closed: boolean = false

  constructor(writable: WritableStream, _readable: ReadableStream) {
    this.writable = writable
    this.writer = writable.getWriter()
    this.encoder = new TextEncoder()

    const abortResponseQueue = responseQueueAborts.get(_readable)
    if (abortResponseQueue) {
      responseQueueAborts.delete(_readable)
      this.responseReadable = _readable
      this.abortSubscribers.push(abortResponseQueue)
      return
    }

    const reader = _readable.getReader()

    // in case the user disconnects, let the reader know to cancel
    // this in-turn results in responseReadable being closed
    // and writeSSE method no longer blocks indefinitely
    this.abortSubscribers.push(async () => {
      await reader.cancel()
    })

    this.responseReadable = new ReadableStream({
      async pull(controller) {
        const { done, value } = await reader.read()
        done ? controller.close() : controller.enqueue(value)
      },
      cancel: () => {
        if (!this.closed) {
          this.abort()
        }
      },
    })
  }

  async write(input: Uint8Array | string): Promise<StreamingApi> {
    try {
      if (typeof input === 'string') {
        input = this.encoder.encode(input)
      }
      await this.writer.write(input)
    } catch {
      // Do nothing. If you want to handle errors, create a stream by yourself.
    }
    return this
  }

  async writeln(input: string): Promise<StreamingApi> {
    await this.write(input + '\n')
    return this
  }

  sleep(ms: number): Promise<unknown> {
    return new Promise((res) => setTimeout(res, ms))
  }

  async close() {
    this.closed = true
    try {
      await this.writer.close()
    } catch {
      // Do nothing. If you want to handle errors, create a stream by yourself.
    }
  }

  async pipe(body: ReadableStream) {
    this.writer.releaseLock()
    try {
      await body.pipeTo(this.writable, { preventClose: true, preventAbort: true })
    } finally {
      this.writer = this.writable.getWriter()
    }
  }

  onAbort(listener: () => void | Promise<void>) {
    this.abortSubscribers.push(listener)
  }

  /**
   * Abort the stream.
   * You can call this method when stream is aborted by external event.
   */
  abort() {
    if (!this.aborted) {
      this.aborted = true
      this.abortSubscribers.forEach((subscriber) => {
        try {
          void Promise.resolve(subscriber()).catch(() => {})
        } catch {
          // Ignore synchronous listener errors.
        }
      })
    }
  }
}
