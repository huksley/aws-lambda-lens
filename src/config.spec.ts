import { config } from './config'
import * as assert from 'assert'
import { logger as log } from './logger'

describe('config.ts', () => {
  it('sensible defaults', () => {
    assert.ok(config.AWS_REGION)
    log.info('config', config)
  })
})
