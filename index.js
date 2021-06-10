'use strict'

const { version } = require('./package.json')
const { createLogger } = require('@everymundo/simple-logr')
const logger = createLogger(version)
const fs = require('fs')
const readline = require('readline')
const { google } = require('googleapis')
const { Client } = require('pg')
let {CONFIG_JSON, GOOGLE_TOKEN_JSON} = process.env
CONFIG_JSON = JSON.parse(CONFIG_JSON);
GOOGLE_TOKEN_JSON = JSON.parse(GOOGLE_TOKEN_JSON);
let { redshiftConfig, googleSheetsConfig, googleApiCredentials } = CONFIG_JSON
console.log(redshiftConfig)
console.log(googleSheetsConfig)
console.log(googleApiCredentials)
console.log(typeof googleApiCredentials)
const { spreadsheetId, sheetName } = googleSheetsConfig;
const SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
const TOKEN_PATH = 'token.json'
let sheets
const valueInputOption = 'RAW'

const handler = async event => {
  logger.info({ event })
  const { data: auth, error } = await authorizeGoogle(googleApiCredentials)
  if (error) {
    console.log('Error authenticating: ' + error)
    return
  }
  console.log('Google Auth done');
  sheets = google.sheets({ version: 'v4', auth })
  try {
    await connectRedshift()
  } catch (e) {
    console.log(e)
    return
  }
  return
}

const authorizeGoogle = async credentials => {
  const {
    installed: {
      client_secret: clientSecret,
      client_id: clientId,
      redirect_uris: redirectUris
    }
  } = credentials
  const oAuth2Client = new google.auth.OAuth2(clientId, clientSecret, redirectUris[0])
  let token
  try {
    token = GOOGLE_TOKEN_JSON;
  } catch (e) {
    console.log('Error reading tokens: ' + e)
  }
  if (token) {
    oAuth2Client.setCredentials(token)
    return {
      data: oAuth2Client,
      error: null
    }
  } else {
    const { data: newoAuth2Client, error } = await getNewToken(oAuth2Client)
    if (error) {
      return {
        data: null,
        error: `Error grabbing new token: ${error}`
      }
    } else {
      return {
        data: newoAuth2Client,
        error: null
      }
    }
  }
}

const getNewToken = async oAuth2Client => {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES
  })
  console.log('Authorize this app by visiting this url:', authUrl)
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  })
  rl.question('Enter the code from that page here: ', async code => {
    rl.close()
    let token
    try {
      const { tokens } = await oAuth2Client.getToken(code)
      token = tokens
    } catch (e) {
      return {
        data: null,
        error: `Error grabbing token: ${e}`
      }
    }
    oAuth2Client.setCredentials(token)
    try {
      fs.writeFileSync(TOKEN_PATH, JSON.stringify(token))
      console.log('Token stored to', TOKEN_PATH)
      return {
        data: oAuth2Client,
        error: null
      }
    } catch (e) {
      return {
        data: null,
        error: `Error writing tokens to file: ${e}`
      }
    }
  })
}

const connectRedshift = async () => {
  redshiftConfig = redshiftConfig
  const client = new Client(redshiftConfig)
  await client.connect()
  console.log('connected at ' + new Date().toLocaleString())
  let res;
  try {
    res = await client.query('select * from viewable_impression_by_airlines');
  } catch (e) {
    client.end()
    return console.log(`Query failed! ${e.stack}`)
  }
  console.log('query done at ' + new Date().toLocaleString())
  client.end()
  const rows = [['airline', 'events']];
  console.log("Number of rows:", res.rows.length)
  for (const row of res.rows) {
    rows.push([row.airline, row.events])
  }
  try {
    await setRows(rows)
  } catch (e) {
    return console.log(`Error setting rows: ${e}`)
  }
  return
}

const setRows = async rows => {
  const values = rows
  const resource = { values }
  let result
  try {
    result = await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${sheetName}!A:B`,
      valueInputOption,
      resource
    })
  } catch (e) {
    return console.log(`Error updating spreadsheet: ${e}`)
  }
  if (result) {
    return console.log(`${result.data.updatedCells} cells updated.`)
  }
}

module.exports = { handler }
