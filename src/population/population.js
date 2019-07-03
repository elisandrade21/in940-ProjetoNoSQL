const log = typeof console === 'undefined' ? print : console.log
const read = typeof require === 'undefined' ? cat : require('fs').readFileSync
const isMongo = typeof require === 'undefined'

function bucketGenerator(elements, properties, mapper) {
    const buckets = {}

    const getBucket = (buckets, values) => {
        let cursor = buckets
        values.forEach((value, i) => {
            if (cursor[value] == undefined) cursor[value] = i === values.length - 1 ? [] : {}
            cursor = cursor[value]
        })
        return cursor
    }

    elements.forEach(element => {
        const values = properties.map(property =>
            typeof property === 'string' ? element[property] : property(element)
        )
        getBucket(buckets, values).push(mapper(element))
    })

    return buckets
}

function gaussianGenerator(mean = 0, std = 1) {
    return ([...new Array(6)].reduce(value => value + Math.random(), 0) - 3) * std + mean
}

function randomDateGenerator(from, to = new Date()) {
    return new Date(+from + Math.random() * (+to - +from))
}

function randomSample(samples) {
    return samples[Math.round(Math.random() * (samples.length - 1))]
}

function gaussianRandomSample(samples, mean, std) {
    mean = mean == undefined ? samples.length / 2 : mean
    std = std == undefined ? samples.length / 4 : std
    return samples[Math.max(0, Math.min(samples.length - 1, Math.round(gaussianGenerator(mean, std))))]
}

const randomCnhs = [...new Array(10000)].map(() => Math.floor(10000000000 + Math.random() * 90000000000))
function randomCnh() {
    return randomCnhs.pop()
}

const rawVehicles = JSON.parse(read('./vehicles.json'))
const colors = ['white', 'gray', 'black', 'silver', 'red', 'blue']
function randomVehicle() {
    return Object.assign({}, randomSample(rawVehicles), {
        year: Math.min(Math.round(gaussianGenerator(2015, 2)), new Date().getFullYear()),
        color: randomSample(colors)
    })
}

const rawPeople = JSON.parse(read('./people.json')).results

const states = [...new Set(rawPeople.map(p => p.location.state))]
const cities = [...new Set(rawPeople.map(p => p.location.city))]
const streets = [...new Set(rawPeople.map(p => p.location.street))]

const driverRatio = 0.05
const rawDrivers = rawPeople.slice(0, Math.floor(rawPeople.length * driverRatio))
const drivers = {}
rawDrivers.forEach(
    rawDriver =>
        (drivers[rawDriver.login.uuid] = {
            uuid: rawDriver.login.uuid,
            name: `${rawDriver.name.first} ${rawDriver.name.last}`,
            email: rawDriver.email,
            pwd: rawDriver.login.password,
            address: {
                state: rawDriver.location.state,
                city: rawDriver.location.city,
                street: rawDriver.location.street
            },
            phone: Math.random() < 0.5 ? [rawDriver.phone, rawDriver.cell] : [rawDriver.phone],
            cnh: {
                number: randomCnh(),
                expire: randomDateGenerator(
                    new Date(new Date().getFullYear() + 1, 1),
                    new Date(new Date().getFullYear() + 5, 12)
                ),
                type: randomSample(['ab', 'b', 'c', 'd'])
            }
        })
)
const driversVehicles = {}
rawDrivers.forEach(rawDriver => (driversVehicles[rawDriver.login.uuid] = randomVehicle()))
const driversStateBuckets = bucketGenerator(
    Object.keys(drivers).map(uuid => drivers[uuid]),
    [d => d.address.state],
    d => d
)

const rawPassengers = rawPeople.slice(Math.floor(rawPeople.length * driverRatio) + 1)
const passengers = {}
rawPassengers.forEach(
    rawPassenger =>
        (passengers[rawPassenger.login.uuid] = {
            uuid: rawPassenger.login.uuid,
            name: `${rawPassenger.name.first} ${rawPassenger.name.last}`,
            email: rawPassenger.email,
            pwd: rawPassenger.login.password,
            address: {
                state: rawPassenger.location.state,
                city: rawPassenger.location.city,
                street: rawPassenger.location.street
            },
            phone: Math.random() < 0.5 ? [rawPassenger.phone, rawPassenger.cell] : [rawPassenger.phone]
        })
)
const passengersStateBuckets = bucketGenerator(
    Object.keys(passengers).map(uuid => passengers[uuid]),
    [d => d.address.state],
    d => d
)

const trips = []
const tripCount = 100000
for (let i = 0; i < tripCount; i++) {
    log(`generating trip ${i}`)
    const state = gaussianRandomSample(states)
    const pickupCity = gaussianRandomSample(cities)
    const destinationCity = gaussianRandomSample(cities)
    const pickupStreet = gaussianRandomSample(streets)
    const destinationStreet = gaussianRandomSample(streets)

    const pickupAddress = { state, city: pickupCity, street: pickupStreet }
    const destinationAddress = { state, city: destinationCity, street: destinationStreet }
    const distance = gaussianGenerator(8, 2)
    const estimatedValue = Math.min(6, 2 + distance * 1.5)
    const finalValue = Math.min(6, estimatedValue * (Math.random() * 0.4 + 0.8))
    const date = randomDateGenerator(new Date(2010, 1, 1))

    const driver = gaussianRandomSample(driversStateBuckets[state])
    const passenger = gaussianRandomSample(passengersStateBuckets[state])

    const vehicle = driversVehicles[driver.uuid]

    const payment = {
        method: randomSample(['credit', 'debit', 'cash']),
        tip: randomSample([1, 2, 5])
    }

    const rating = Math.min(Math.floor(gaussianGenerator(4, 1)), 5)

    trips.push({
        pickupAddress,
        destinationAddress,
        distance,
        estimatedValue,
        finalValue,
        date,
        driver,
        passenger,
        vehicle,
        payment,
        rating
    })
}

log('generation done')

// MONGO ONLY
if (isMongo) {
    // use carservice
    const carServiceDB = db.getSiblingDB('carservice')
    db = carServiceDB

    db.createCollection('trips')
    db.trips.insert(trips)
    log('persistence done')
}
