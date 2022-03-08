import express from 'express'
import bodyParse from 'body-parser'

const app = express()
app.use(bodyParse.json())
const {Kafka} = require('kafkajs')

app.post('/produce', async (req: any, resp: any) => {
    console.log(req.body)
    const kafka = new Kafka({
        clientId: 'my-app',
        brokers: ['localhost:9092'],
    })
    const producer = kafka.producer()

    await producer.connect()
    await producer.send({
        topic: 'test-topic',
        messages: [
            {value: req.body.hello},
        ],
    })

    await producer.disconnect()
    resp.sendStatus(200)
});

app.get('/sufyan', (req: any, resp: any) => {
    resp.send('sufyan');
});
app.get('/messages', async (req: any, resp: any) => {
    const kafka = new Kafka({
        clientId: 'my-app',
        brokers: ['localhost:9092'],
    })
    const consumer = kafka.consumer({groupId: 'test-group'})
    await consumer.connect()
    await consumer.subscribe({topic: 'test-topic', fromBeginning: true})
    await consumer.run({
        eachMessage: async ({message}:any) => {
            console.log({
                value: message.value.toString(),
            })
        },
    })
});


app.listen(3001, () => {
    console.log("Started");
})
