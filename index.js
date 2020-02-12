const express = require('express')
const pg = require('pg')
const compression = require('compression');
const app = express();
const config = require('config');
// configs come from standard PostgreSQL env vars
// https://www.postgresql.org/docs/9.6/static/libpq-envars.html

/*
Shazil Arif
EQ works product role work sample/challenge
API Rate Limiting
*/

let queued =  []; //here we will queue in users who exceed the request limit
let requests = {}; //quick look up for ip address and the number of requests that address made
const MAX_REQ = 8; //max reqeuests, arbitrary number
const TIMEOUT = 30000; //30 second timeout, arbitrary

const pool = new pg.Pool({
  user: 'readonly',
  host: config.get('host'), 
  database: 'work_samples',
  password: config.get('password'),
  port: 5432,
})


const queryHandler = (req, res, next) => {
  pool.query(req.sqlQuery).then((r) => {
    return res.json(r.rows || [])
  }).catch(next)
}

app.use(compression());

const free_user = (route,ip) => setTimeout(free_user_helper,TIMEOUT)

const free_user_helper = (route,ip) =>{
  let to_free = queued.shift() //following a First in first out manner, unblock the first user
  requests[to_free.endpoint][to_free.ip]= 0; //reset users request count to 0
}

/*
requests = {
  "/events/hourly":{
    a mapping between ip address and number of requests made
    "192.1.21.90":5,
    "192.1.31.13":8
  },
  "/events/daily":{
    "192.1.21.90":1,
    "192.1.31.13":1
  }
}
*/

const rate_limiter = (req,res,next) =>{

  let calling_url = req.url;
  let request_ip = req.connection.remoteAddress;
  let in_queue = requests[calling_url];
  let x = `${request_ip}`
  console.log(requests);

  if(in_queue && requests[calling_url][request_ip] >= MAX_REQ){

    const next_in_queue = {
      endpoint:calling_url,
      ip:request_ip
    }

    //queue the user so we can remove them in a First in First Out (FIFO) manner
    queued.push(next_in_queue);

    process.nextTick(free_user); //schedule users to be free'd immediately after
    return res.status(429).send(`${MAX_REQ} Requests limit exceeded. Please wait 30 seconds. you can still visit other http endpoints, each has its own ${MAX_REQ} limit`)
  }
  if(in_queue){
    requests[calling_url][request_ip]++; //increase count by one if user already requested before
  }
  else {
    requests[calling_url] = { 
      [request_ip]:1  //if first time requesting, set users count to 1
    }; 
  }

  return next(); //pass control to the callback for api endpoint
}

app.use(rate_limiter) //use our middleware above^, will apply to all the routes

app.get('/',(req, res) => {
  res.send('Welcome to EQ Works 😎')
})


app.get('/events/hourly', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, events
    FROM public.hourly_events
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/events/daily', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, SUM(events) AS events
    FROM public.hourly_events
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/stats/hourly', (req, res, next) => {
  req.sqlQuery = `
    SELECT date, hour, impressions, clicks, revenue
    FROM public.hourly_stats
    ORDER BY date, hour
    LIMIT 168;
  `
  return next()
}, queryHandler)

app.get('/stats/daily', (req, res, next) => {
  req.sqlQuery = `
    SELECT date,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(revenue) AS revenue
    FROM public.hourly_stats
    GROUP BY date
    ORDER BY date
    LIMIT 7;
  `
  return next()
}, queryHandler)

app.get('/poi', (req, res, next) => {
  req.sqlQuery = `
    SELECT *
    FROM public.poi;
  `
  return next()
}, queryHandler)

app.listen(process.env.PORT || 5432, (err) => {
  if (err) {
    console.error(err)
    process.exit(1)
  } else {
    console.log(`Running on ${process.env.PORT || 5432}`)
  }
})

// last resorts
process.on('uncaughtException', (err) => {
  console.log(`Caught exception: ${err}`)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.log('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})
