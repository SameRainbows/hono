import { env, getRuntimeKey } from '../../src/helper/adapter'
import { streamSSE } from '../../src/helper/streaming'
import { Hono } from '../../src/hono'
import { getColorEnabledAsync } from '../../src/utils/color'

const app = new Hono()

app.get('/', (c) => c.text(`Hello from ${getRuntimeKey()}`))

app.get('/env', (c) => {
  const { NAME } = env<{ NAME: string }>(c)
  return c.text(NAME)
})

app.get('/color', async (c) => {
  return c.text((await getColorEnabledAsync()) ? 'True' : 'False')
})

app.get('/events', (c) => {
  return streamSSE(c, async (stream) => {
    await stream.writeSSE({ data: 'first' })
    await stream.writeSSE({ data: 'second' })
  })
})

export default app
