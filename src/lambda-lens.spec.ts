import { lens } from './lambda-lens'

describe('lambda-lens.ts', () => {
  it('list all groups', () => {
    return lens({} as AWSLambda.APIGatewayEvent)
  })
})
