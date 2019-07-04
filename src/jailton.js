//import { emit } from "cluster";
function map() {
    emit(this.pickupAddress.state,[this.passenger.name, this.distance])
}

function reduce(key, values) {
    let currentMaxDistance = 0
    let passengerName = null
    for (let v of values) {
         if (v[1] > currentMaxDistance) {
             currentMaxDistance = v [1]
             passengerName = v[0]
         }
         
        }
        return passengerName
}

db.trips.mapReduce(map,reduce, {out: 'bestPassenger'})