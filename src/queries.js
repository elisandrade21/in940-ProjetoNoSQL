// USE
// Para mudar para outro banco de dados, usamos o comando "use nomeDoBanco". Se o banco de dados não 
//existir o MongoDB o criará assim que forem incluídos dados nele. O Shell deve apresentar a  
// mensagem: switched to db meuMongoDB
// use carservice
// 
db = db.getSiblingDB('carservice')

// proporção de viagens cujo rating foi 5
print(db.trips.count({ rating: 5 }) / db.trips.count())

// FIND
// Executes a query and returns the first batch of results and the cursor id, from which the client can construct a cursor.
// listar passageiros das viagens com 12 ou mais quilometros de distância
db.trips.find({ distance: { $gte: 12 } }, { 'passenger.name': 1, distance: 1 }).pretty()

// listar os estados e a quantidade de viajens feitas, ordenado pela quantidade
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

// contar viagens cuja a cidade do passageiro é igual a do motorista.
db.trips.count({ $expr: { $eq: ['$driver.address.city', '$passenger.address.city'] } })

// contar a quantidade de viagens por estado cujo o valor final é maior que o valor estimado
db.trips.aggregate([
    {
        $group: {
            _id: '$pickupAddress.state',
            count: {
                $sum: {
                    $cond: {
                        if: { $gt: ['$estimatedValue', '$finalValue'] },
                        then: 1,
                        else: 0
                    }
                }
            }
        }
    },
    { $sort: { count: -1 } }
])

// UNWIND
// Operaçao sobre arrays

// Deconstructs an array field from the input documents to
// output a document for each element. Each output 
// document is the input document with the value of the array 
//  field replaced by the element.
// Mostra um registro (repetindo os valores) para cada linha do array
// Utilizando db.movieDatails
db.movieDetails.aggregate(
{ $unwind : "$countries" },
{ $project : {
        year : 1 ,
        title : 1 ,
        director : 1,
        countries : 1
    }}
);



//criar um índice
db.trips.createIndex({'passenger.name': "text"})

// retornar os passageiros que tem sobrenome moura
db.trips.find({ $text: { $search: "'moura'" }}, {passenger: 1}).pretty()

