const express = require('express')
const pg = require('pg')
const app = express();
const config = require('config');
// configs come from standard PostgreSQL env vars
// https://www.postgresql.org/docs/9.6/static/libpq-envars.html

/*
Shazil Arif
EQ works product role work sample/challenge
API Rate Limiting
*/

let queued =  []; //here we will queue in users who exceed the request limit and unqueue them in a first in first out manner
let requests = {}; //quick look up for ip address and the number of requests that address made


/* The idea is the following, requests object will look like:

a mapping between api endpoints and an object containing request ip addresses and how many requests each address made
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

const MAX_REQ = 8; //max requests, arbitrary number to simulate a limit
const TIMEOUT = 30000; //30 second timeout, arbitrary number to simulate 



//connect to postgres database
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

//call free_user that will call helper/axuliary function with a timeout of 30 seconds
const free_user = () => setTimeout(free_user_helper,TIMEOUT)

//helper/auxiliary function to unqeue users who have been blocked from making requests
const free_user_helper = () =>{
  let to_free = queued.shift() //following a First in first out manner, unblock the first user
  requests[to_free.endpoint][to_free.ip]= 0; //reset users request count to 0
}

//middleware
const rate_limiter = (req,res,next) =>{

  //get the route that called the middleware so we know which route to increase the number of requests for
  let calling_url = req.url;
  console.log("URL:"+req.url);
  //get current requests ip address

  let request_ip = req.connection.remoteAddress;

  //get the object for the route that called the middleware
  let current_route = requests[calling_url];
  
  console.log(requests); //for debugging track the requests of each endpoint at any time, please view output in terminal/console

  /*if current route is defined then lookup users ip and get their request count for the specific route
  if greater than max allowed, then block*/
  if(current_route && requests[calling_url][request_ip] >= MAX_REQ){

    /* create a new object with route name and user ip 
    this will be used to add users to a queue of blocked users 
    they will then be unqeued*/

    const next_in_queue = {
      endpoint:calling_url,
      ip:request_ip
    }

    //queue the user so we can remove them in a First in First Out (FIFO) manner
    queued.push(next_in_queue);

    process.nextTick(free_user); //schedule users to be free'd immediately our middlware finishes executing

    //send a response to our client
    return res.status(429).send(`${MAX_REQ} Requests limit exceeded. Please wait 30 seconds. you can still visit other http endpoints, each has its own ${MAX_REQ} request limit. See the main / route to track ur requests count. If you exceeded ${MAX_REQ} on the main route as well then wait 30 seconds till it resets`)
  }

  //if the user/ip has no exceeded the max number of requests, increment their count
  if(current_route){
    requests[calling_url][request_ip]++; //increase count by one if user already requested before
  }

  //if user/ip has not made a request to the route yet, set their count for the route to 1
  else {
    requests[calling_url] = { 
      [request_ip]:1  //if first time requesting, set users count to 1
    }; 
  }

  return next(); //pass control to the callback/handler for api endpoint
}

app.use(rate_limiter) //use our middleware above^, will apply to all the routes


app.get('/',(req, res) => {

  /*only for demo purposes, in practice, the html responses should be kept in a seperate file*/

  //get the keys of requests object which are the routes
  let routes = Object.keys(requests);
  let response = ``;

  //iterate over each route
  for(let i = 0; i < routes.length; i++){

    //get the current request ip
    const user_ip = req.connection.remoteAddress

    //get current users request count for i'th route
    if(user_ip in requests[routes[i]]){
      let name = routes[i];
      let count = requests[routes[i]][user_ip];

      //build a html reponse 
      if(count >= MAX_REQ)
        response += `<li> route: ${name} --  requests made: ${count}, exceeded - waiting to reset <li>`;
      else
        response += `<li> route: ${name} --  requests made: ${count}<li>`;
    }
  }
  //send the response to the client
  return res.send(` Welcome to EQ Works 😎 listed are the endpoints and how many requests you've made to each. call endpoints for their count to show. source code: https://github.com/shazil-arif/eq-work-sample  ${response}`)
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

//deal with invalid requests

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
