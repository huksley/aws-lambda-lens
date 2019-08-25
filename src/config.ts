import * as dotenv from 'dotenv'
import * as R from 'ramda'
import 'source-map-support/register'

export const defaultConfig = {
  NODE_ENV: 'development' as 'development' | 'product',
  LOG_LEVEL: 'info' as 'info' | 'debug' | 'warn' | 'error',
  AWS_REGION: 'eu-west-1',

  /** Should run e2e tests, potentially spending money? */
  TEST_E2E: false,
  /** Bucket for images to run tests against */
  E2E_IMAGE_BUCKET: 'sample-bucket',
  IMAGE_BUCKET: 'sample-bucket',
  // AWS Step Function workflow to launch
  WORKFLOW_ARN: '',
}

type defaultConfigKey = keyof typeof defaultConfig

/** Converts specific keys to boolean */
const toBoolean = (o: typeof defaultConfig, k: defaultConfigKey[]): typeof defaultConfig => {
  for (const kk of k) {
    o[kk] = typeof o[kk] === 'string' ? Boolean(o[kk]) : o[kk]
  }
  return o
}

/** Converts specific keys to number */
const toNumber = (o: typeof defaultConfig, k: defaultConfigKey[]): typeof defaultConfig => {
  const oo = o as any
  for (const kk of k) {
    oo[kk] = typeof o[kk] === 'string' ? Number(o[kk]) : o[kk]
  }
  return o
}

/**
 * Typed, configurable instance of application config. Use environment or .env files to define variables.
 */
export const config = toNumber(
  toBoolean(
    {
      ...defaultConfig,
      ...(dotenv.config().parsed || R.pick(R.keys(defaultConfig), process.env)),
    },
    ['TEST_E2E'],
  ),
  [],
)
