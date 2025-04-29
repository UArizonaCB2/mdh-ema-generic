const mdh = require('./mdh')
const secretManager = require('./SecretsManager')
require('dotenv').config()

const customFields = {
  ema_max : 'ema_max',  // The maximum number of EMA per category
  ema_metadata : 'ema_metadata',  // Prefixed by {x}. Holds meta-data of random numbers already served.
  ema_random : 'ema_random', // Prefixed by {x}. Holds the random number of the EMA to serve.
  ema_categories : 'ema_categories', //Total number of EMA categories to serve.
  ema_status : 'ema_status'
}

async function main(args) {
  let rksServiceAccount = null
  let privateKey = null
  let rksProjectId = null

  const secretName = process.env.AWS_SECRET_NAME

  // If we are in production system then MDH configuration will get loaded from the secrets manager.
  if (process.env.NODE_ENV === 'production') {
    let secret = await secretManager.getSecret(secretName)
    secret = JSON.parse(secret)
    rksProjectId = secret['RKS_PROJECT_ID']
    rksServiceAccount = secret['RKS_SERVICE_ACCOUNT']
    privateKey = secret['RKS_PRIVATE_KEY']
  }
  else {
    // Local / Non-production environment.
    // If We have passed the service account and private key path in the environment use that.
    if (process.env.RKS_SERVICE_ACCOUNT && process.env.RKS_PRIVATE_KEY) {
      console.log('Using MDH credentials from environment variables')
      rksServiceAccount = process.env.RKS_SERVICE_ACCOUNT

      rksProjectId = process.env.RKS_PROJECT_ID
      privateKey = process.env.RKS_PRIVATE_KEY
    }
    else {
      console.log('Fatal Error: RKS service account and RKS private key must be set in env variables.')
      return null
    }
  }

  // Needed when passing and storing the keys in \n escaped single lines.
  privateKey = privateKey.replace(/\\n/g, '\n')

  const token = await mdh.getAccessToken(rksServiceAccount, privateKey)
  if(token == null) {
    return null
  }

  const participants = await mdh.getAllParticipants(token, rksProjectId)
  for (const participant of participants.participants) {
    let status = 'passed'
    const ema_max = safeIntConvert(getCustomField(participant, customFields.ema_max), 0)
    const ema_categories = parseInt(getCustomField(participant, customFields.ema_categories), 10)
    if(Number.isNaN(ema_categories)) {
      status = 'failed - Check value for EMA Category. It should be an Integer. Make sure there are no leading or trailing spaces'
      const payload = createParticipantError(participant, status)
      await mdh.updateParticipant(token, rksProjectId, payload)
      return false
    }

    // Random EMA logic for each category of EMA.
    for (let a=1; a <= ema_categories; a++) {
      let ema_metadata = getCustomField(participant, customFields.ema_metadata+a.toString())
      let ema_done = []
      if (ema_metadata.length > 0) {
        ema_done = ema_metadata.split(',')
      }

      let random_number = bruteRandomExclude(ema_done, ema_max)
      // If we have exhausted all the random numbers to pick from, we clear the holder and start all over again.
      if (Number.isNaN(random_number)) {
        ema_done = []
        random_number = bruteRandomExclude(ema_done, ema_max)
      }
      // Append the new random value to the EMA.
      ema_done.push(random_number.toString())

      const payload = {
        'id': participant.id,
        'customFields': {}
      }
      payload.customFields[customFields.ema_metadata+a.toString()] = ema_done.join(',')
      payload.customFields[customFields.ema_random+a.toString()] = random_number
      await mdh.updateParticipant(token, rksProjectId, payload)
    }
  }

  return true
}

function createParticipantError(participant, message) {
  const payload = {
    'id': participant.id,
    'customFields': {}
  }

  payload.customFields[customFields.ema_status] = message

  return payload
}

//TODO: Sure I can add a dictionary, but the `max` value is tiny. Let's burn some compute.
function bruteRandomExclude(arr, max, maxTries=10) {
  arr = arr.map((x) => parseInt(x, 10)) // Convert all elements to integers.
  arr = arr.map((x) => Number.isNaN(x)?0:x) // Handle NaN values
  for (let a=0; a < maxTries; a++) {
    const temp = getRandom(1, max, 1)
    if (!elementIn(arr, temp)) {
      // Lucky us we found a random number not in the list. Let's return it and go home.
      return temp
    }

    // If we are here it means we could not find anything. Need to do this the hard way.
    // Return the first element that we could not find
    for (i=1; i <= max; i++) {
      if (!elementIn(arr, i)) {
        return i
      }
    }

    /*
     * If we are here it means there is no possible value that we can return which we have not already returned.
     * We just go ahead and return a NaN at this point and let the called know this.
     */
    return Number.NaN
  }
}

function elementIn(arr, ele) {
  for (const a of arr) {
    if (a === ele) {
      return true
    }
  }

  return false
}

/*
 * Safely convert a stirng to integer replacing it with default value if it fails.
 * @param {string} string - string to parse to integer.
 * @param {string} defaultVal - Default value to return in case of conversion failure.
 * @returns {int} converted integer value, or `defaultVal` in case of failure.
 */
function safeIntConvert(string, defaultVal) {
  const val = parseInt(string, 10)
  if (Number.isNaN(val)) {
    return defaultVal
  }

  return val
}

/*
 * Get the specified custom field from the participant.
 * @param {object} participant - MDH participant object.
 * @param {string} fieldName - Name of the custom field.
 * @returns {string} value of the custom field if found, null otherwise.
 */
function getCustomField(participant, fieldName) {
  if (fieldName in participant.customFields) {
    return participant.customFields[fieldName]
  }

  return null
}

/*
 * Method which returns a random number inclusive of the bounds.
 * @param {int} min
 * @param {int} max
 * @param {int} defaultVal - Returned in case of a failure
 * @returns {int} random integer within the bounds.
 */
function getRandom(min, max, defaultVal) {
  if (max < min) {
    return defaultVal
  }
  return Math.floor(Math.random() * (max - min + 1) + min)
}

exports.main = main
