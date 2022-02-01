const amqp = require("amqplib/callback_api");

require('dotenv').config();


import ResponseModel from './responseModel';
import Exchange from "./exchanges";



/**
 * interface for giving direction to the queue message
 */
interface QueueURLMapValue {
    queueName: string;
    OnSuccessTopicsToPush: string[];
    OnFailureTopicsToPush: string[];
}


export class Broker {
    private static instance: Broker;
    private queueURLMap: { [id: string]: QueueURLMapValue } = {};
    private channel = null;
    private rabbitmqUrl: string = process.env.BROKER_URL || `amqp://127.0.0.1:5672`;
    private topicNames: string[] = [];

    private constructor() {
        this.init_Broker();
    }

    public static getInstance(): Broker {
        if (!Broker.instance) {
            Broker.instance = new Broker();
        }
        return Broker.instance;
    }

    /**
     * it will initiate all the exchanges.
     */

    private async init_Broker() {
        try {
            console.log("connecting to rabbitmq ...", this.rabbitmqUrl);
            amqp.connect(this.rabbitmqUrl, (err, connection) => {
                connection.createChannel((err, channel) => {
                    this.channel = channel;
                    const topics = Exchange.Topics;
                    /**
                     * Model of topic
                     * 
                     * {
                            TopicName: "EMPLOYEE_ADD",
                            Publishers: ["API_GATEWAY_SERVICE"],
                            Method: "POST",
                            Subscribers: [
                                {
                                Service: "IOT_SERVICE",
                                OnSuccessTopicsToPush: ["EMPLOYEE_ADDED"],
                                OnFailureTopicsToPush: ["ERROR_RECEIVER"],
                                QueueName: "EMPLOYEE_ADD-IOT_SERVICE"
                                },
                            ],
                        }
                     */
                    for (let i = 0; i < topics.length; i++) {
                        let topic = topics[i];
                        let topicName = topic.TopicName;
                        this.topicNames.push(topicName);
                        //create channel for given topic name
                        this.channel.assertExchange(topicName, "fanout", {
                            durable: true,
                        });
                        let subscribers = topic.Subscribers;
                        for (let j = 0; j < subscribers.length; j++) {
                            let subscriber = subscribers[j];
                            let queueName = subscriber.QueueName;
                            this.channel.assertQueue(queueName, {
                                exclusive: false,
                            });
                            //bind the queue with exchange with queueName
                            this.channel.bindQueue(queueName, topicName, "");
                            let queueURLMapValue = {
                                queueName: queueName,
                                OnSuccessTopicsToPush: subscriber.OnSuccessTopicsToPush,
                                OnFailureTopicsToPush: subscriber.OnFailureTopicsToPush,
                            };
                            this.queueURLMap[queueName] = queueURLMapValue;
                        }
                    }
                });
            });
        } catch (error) {
            console.log(error.message, 'Ckech you rabbitmq is running ...');
        }
    }


    /**
     * this will publish the message to respective topic name.
     */


    public publishMessageToTopic(topicName: string, message: any): ResponseModel<{}> {
        console.log('message recieved in the broker to publish : ', message);
        // before publish into topic we need to make strigified message to buffer
        const data = Buffer.from(JSON.stringify(message));
        // publish the message to topic
        let response: ResponseModel<{}>;
        if (this.topicNames.includes(topicName)) {
            this.channel.publish(topicName, "", data);
            response = new ResponseModel(200, 'SUCCESS', 'POST', `Successfully published into Topic Name : ${topicName} `, {});
        } else {
            response = new ResponseModel(400, 'FAILED', 'POST', `Unalble to publish to Topic Name : ${topicName} `, {});
        }
        return response;
    }

    /**
     * Listen to a perticular queue
     * queueName = topicName + '-' + serviceName
    */

    public async listenToService(topicName, serviceName, callBack) {
        try {
            const queueURLMapValue = this.queueURLMap[topicName + "-" + serviceName];
            const queueName = queueURLMapValue.queueName;
            // consume message from queue
            this.channel.consume(
                queueName,
                (msg) => {
                    if (msg.content) {
                        let message = JSON.parse(msg.content);
                        callBack({
                            message,
                            OnSuccessTopicsToPush: queueURLMapValue.OnSuccessTopicsToPush,
                            OnFailureTopicsToPush: queueURLMapValue.OnFailureTopicsToPush
                        });
                    }
                },
                { noAck: true }
            );
        } catch (e) {
            setTimeout(() => {
                this.listenToService(topicName, serviceName, callBack);
            }, 5000)
        }
    }

    /**
     * Listen to a perticular service with a callback
     */

    public listenToServices(serviceName, callback) {
        let topics = Exchange.Topics;
        for (let i = 0; i < topics.length; i++) {
            let topic = topics[i];
            let topicName = topic.TopicName;
            let subscribers = topic.Subscribers;
            for (let j = 0; j < subscribers.length; j++) {
                let subscriber = subscribers[j];
                let vServiceName = subscriber.Service;
                if (vServiceName === serviceName) {
                    this.listenToService(topicName, serviceName, callback);
                }
            }
        }
    }
}
