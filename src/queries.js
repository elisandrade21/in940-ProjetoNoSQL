// use carservice
db = db.getSiblingDB('carservice')

// proporção de viagens cujo rating foi 5
print(db.trips.count({ rating: 5 }) / db.trips.count())

// listar passageiros das viagens com 12 ou mais quilometros de distância
db.trips.find({ distance: { $gte: 12 } }, {'passenger.name': 1, 'distance': 1}).pretty()

// listar os estados por maior quantidade de viagens feitas
db.trips.aggregate([{ $group: { _id: '$pickupAddress.state', count: { $sum: 1 } } }, { $sort: { count: -1 } }])

// listar motoristas e seus estados, ordenados pelo faturamento do motorista
db.trips.aggregate([
    {
        $group: {
            _id: { name: '$driver.name', state: '$driver.address.state', uuid: '$driver.uuid' },
            total: { $sum: '$finalValue' }
        }
    },
    { $sort: { total: -1 } }
])

// listar motoristas do estado de pernambuco apenas, ordenados pelo faturamento do motorista
db.trips.aggregate([
    { $match: { 'driver.address.state': 'pernambuco' } },
    {
        $group: {
            _id: { name: '$driver.name', state: '$driver.address.state', uuid: '$driver.uuid' },
            total: { $sum: '$finalValue' }
        }
    },
    { $sort: { total: -1 } }
])

// motoristas que possuem carro a partir de 2018
db.trips.aggregate([
    { $match: { 'vehicle.year': { $gte: 2018 } } },
    { $group: { _id: { uuid: '$driver.uuid', carYear: '$vehicle.year' } } }
])

// quantidade de viagens que os passageiros fizeram entre 2015 e 2017
db.trips.aggregate([
    { $match: { $and: [{ date: { $gte: new Date(2015, 1) } }, { date: { $lt: new Date(2019, 1, 1) } }] } },
    { $group: { _id: { uuid: '$passenger.uuid', name: '$passenger.name' }, count: { $sum: 1 } } },
    { $sort: { count: -1 } }
])

// distancia média percorrida por motoristas por estado
db.trips.aggregate([
    { $group: { _id: '$pickupAddress.state', avgDistance: { $avg: '$distance' } } },
    { $sort: { avgDistance: -1 } }
])

// distancia max percorrida por motoristas por estado 
db.trips.aggregate([
    { $group: { _id: '$pickupAddress.state', maxDistance: { $max: '$distance' } } },
    { $sort: { maxDistance: -1 } }
])

// primeiros 20 motoristas que possuem carros a partir de 2015

db.trips.aggregate([
    { $match: { 'vehicle.year': { $gte: 2015 } } },
    { $group: { _id: { uuid: '$driver.uuid', carYear: '$vehicle.year' } } },
    {$limit : 20} 
])

// retornar o passageiro que tem sobrenome moura

db.trips.createIndex({name: "text"})

db.trips.find({ $text: { $search: "'moura'" }})



