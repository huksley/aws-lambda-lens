import { apiResponse, findPayload, decode } from './util'
import { Context as LambdaContext, APIGatewayEvent, Callback as LambdaCallback } from 'aws-lambda'
import { logger } from './logger'
import { config } from './config'
import { CloudWatchLogs } from 'aws-sdk'
import * as moment from 'moment'
import * as t from 'io-ts'
import * as uuidV4 from 'uuid/v4'
import * as PouchDB from 'pouchdb'
import * as findPlugin from 'pouchdb-find'
import { OutputLogEvent } from 'aws-sdk/clients/cloudwatchlogs'
PouchDB.plugin(findPlugin)

const logs = new CloudWatchLogs({
  signatureVersion: 'v4',
  region: config.AWS_REGION,
})

/** Invoked on API Gateway GET event */
export const getHandler = (
  event: APIGatewayEvent,
  context: LambdaContext,
  callback: LambdaCallback,
) => {
  logger.info(
    'event(' +
      typeof event +
      ') ' +
      JSON.stringify(event, null, 2) +
      ' context ' +
      JSON.stringify(context, null, 2),
  )

  const payload = findPayload(event)
  logger.info(`Using payload`, payload)

  try {
    const request = payload as AWSLambda.APIGatewayEvent
    return lens(request)
  } catch (err) {
    logger.warn('Failed to process event', err)
    apiResponse(event, context, callback).failure('Failed to process event: ' + err.message)
  }
}

export const logGroupName2Lambda = (groupName: string) => {
  return new Promise<string>((resolve, reject) => {
    if (groupName.startsWith('/aws/lambda/')) {
      resolve(groupName.substring(groupName.lastIndexOf('/') + 1))
    } else {
      reject('Invalid log group name: ' + groupName)
    }
  })
}

// YYYY/MM/DD
const streamNameParser = /([0-9]{4}\/[0-9]{2}\/[0-9]{2})\/.*/g

export const logStreamName2Date = (streamName: string) => {
  return new Promise<string>((resolve, reject) => {
    const dateResult = new RegExp(streamNameParser).exec(streamName)
    if (dateResult) {
      return resolve(dateResult[1].replace('/', '-'))
    } else {
      return reject('Failed to parse ' + streamName)
    }
  })
}

export const logStreamName2InstanceId = (streamName: string) => {
  return new Promise<string>((resolve, reject) => {
    if (streamName.indexOf(']') > 0) {
      resolve(streamName.substring(streamName.indexOf(']') + 1))
    } else {
      reject('Failed to parse ' + streamName)
    }
  })
}

const reportParser = /.*REPORT RequestId: ([a-z0-9-]+)([ \t]Init Duration: ([0-9.]+) ms){0,1}[ \t]+Duration: ([0-9.]+) ms[ \t]+Billed Duration: ([0-9.]+) ms[ \t]+Memory Size: ([0-9]+) MB[ \t]+Max Memory Used: ([0-9]+) MB.*/g

const MessageDataType = t.type({
  requestId: t.string,
  initDuration: t.union([t.number, t.undefined]),
  duration: t.number,
  billedDuration: t.number,
  requestedMemory: t.number,
  consumedMemory: t.number,
})

export type MessageData = t.TypeOf<typeof MessageDataType>

export const logStreamRecord2Object = (msg: string) => {
  return new Promise<MessageData>((resolve, reject) => {
    let result = new RegExp(reportParser).exec(msg)
    if (result) {
      resolve(
        decode(MessageDataType, {
          requestId: result[1],
          initDuration: result[3] ? parseFloat(result[3]) : undefined,
          duration: parseFloat(result[4]),
          billedDuration: parseFloat(result[5]),
          requestedMemory: parseFloat(result[6]),
          consumedMemory: parseFloat(result[7]),
        }),
      )
    } else {
      reject('Failed to parse ' + msg)
    }
  })
}

const parseEvents = (
  db: PouchDB.Database,
  logGroupName: string,
  logStreamName: string,
  events: OutputLogEvent[],
  stats: any,
) => {
  return events
    .filter(ev => ev.message!.startsWith('REPORT RequestId'))
    .map(async ev => {
      return logGroupName2Lambda(logGroupName)
        .then(lambdaName => {
          return logStreamName2Date(logStreamName!)
            .then(eventDate => {
              return logStreamName2InstanceId(logStreamName)
                .then(instanceId => {
                  return logStreamRecord2Object(ev.message!)
                    .then(async r => {
                      const rec = {
                        ...r,
                        _id: uuidV4(),
                        timestamp: ev.timestamp,
                        lambdaName,
                        eventDate: eventDate,
                        instanceId,
                      }
                      stats[lambdaName]++
                      await db.put(rec).catch(err => logger.warn(err))
                      return rec
                    })
                    .catch(err => logger.warn(err) && null)
                })
                .catch(err => logger.warn(err) && null)
            })
            .catch(err => logger.warn(err) && null)
        })
        .catch(err => logger.warn(err) && null)
    })
}

export const lens = async (event: AWSLambda.APIGatewayEvent) => {
  logger.info('Got event', event)
  const db = new PouchDB('stats')
  await db.createIndex({
    index: {
      fields: ['lambdaName', 'eventDate'],
    },
  })
  const stats = {}
  return logs
    .describeLogGroups({
      limit: 50,
    })
    .promise()
    .then(r => {
      logger.debug('Got log groups response', r)
      return r.logGroups!.map(g => {
        const prefix = moment().format('YYYY/MM/DD') + '/'
        logger.debug('Log group ' + prefix, g.logGroupName)
        return logs
          .describeLogStreams({
            logGroupName: g.logGroupName!,
            descending: true,
            // Only today
            logStreamNamePrefix: prefix,
            limit: 3,
          })
          .promise()
          .then(l => {
            logger.debug('Got log streams response', l)
            return l.logStreams!.map(s => {
              logger.debug(g.logGroupName + ': Got stream', s.logStreamName)
              logs
                .getLogEvents({
                  logGroupName: g.logGroupName!,
                  logStreamName: s.logStreamName!,
                  limit: 100,
                })
                .promise()
                .then(events => {
                  logger.debug('Got events', events)
                  return Promise.all(
                    parseEvents(db, g.logGroupName!, s.logStreamName!, events.events!, stats),
                  )
                })
                .catch(err => logger.warn(err) && null)
            })
          })
          .catch(err => logger.warn(err) && null)
      })
    })
    .catch(err => logger.warn(err) && null)
    .then(_ => {
      logger.info('Stats: ', stats)
    })
}
