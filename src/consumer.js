import { Kafka } from 'kafkajs';
import mongoose from 'mongoose';

import userModel from './models/user.js';
import config from './config.js';

await mongoose
  .connect(config.db.uri)
  .then(() => console.log('Connected to DB'));

const consumeMessage = async (consumer) => {
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      try {
        const msg = JSON.parse(message.value.toString());
        if (topic == 'INSERT') {
          const isExist = await userModel.exists({
            username: msg.data.username,
          });
          if (isExist) throw new Error('Username already exist');
          await userModel.create(msg.data);
        }
        if (topic == 'UPDATE') {
          const isExist = await userModel.exists({ _id: msg.data.userId });
          if (!isExist) throw new Error('User Not Found!');
          await userModel.findByIdAndUpdate(msg.data.userId, msg.data.user);
        }
        if (topic == 'DELETE') {
          const isExist = await userModel.exists({ _id: msg.data.userId });
          if (!isExist) throw new Error('User Not Found!');
          await userModel.deleteOne({ _id: msg.data.userId });
        }
        consumer.logger().info(JSON.parse(message.value.toString()));
        // console.log({
        //   topic: topic,
        //   value: JSON.parse(message.value.toString()),
        // });
      } catch (error) {
        consumer.logger().error(error);
      }
    },
  });
};

try {
  const client = new Kafka({
    clientId: 'users-test',
    brokers: ['localhost:9092'],
  });

  const consumer = client.consumer({ groupId: 'my-group' });

  await consumer.connect();

  await consumer.subscribe({ topic: 'UPDATE', fromBeginning: true });
  await consumer.subscribe({ topic: 'INSERT', fromBeginning: true });
  await consumer.subscribe({ topic: 'DELETE', fromBeginning: true });

  consumeMessage(consumer);
} catch (err) {
  console.log(err);
}
