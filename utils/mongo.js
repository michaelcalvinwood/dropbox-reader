require('dotenv').config();
const { MongoClient } = require('mongodb');

const db = {
  connected: false,
  client: null
};

async function connectToMongo() {
  if (!db.connected) {
    db.client = new MongoClient(process.env.MONGO_URL, {
      useUnifiedTopology: true
    });

    db.client.on('open', _ => {
      db.connected = true;

      console.log('Connected to MongoDB');
    });

    db.client.on('topologyClosed', _ => {
      db.connected = false;

      console.log('Disconnected from MongoDB');
    });

    await db.client.connect();
  }
}

async function insertDocument(_id, data) {
  try {
    if (!db.connected) {
      await connectToMongo();
    }

    const collection = db.client.db('dv').collection('metadata');

    return collection.insertOne(
      {
        _id,
        ...data
      },
      {
        forceServerObjectId: false
      }
    );
  } catch (err) {
    console.error(`Error inserting document: ${err}`);
  }
}

async function closeConnection() {
  if (db.client && db.connected) {
    await db.client.close();
  }
}

module.exports = {
  insertDocument,
  closeConnection
};
