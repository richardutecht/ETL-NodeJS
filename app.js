import { MongoClient } from 'mongodb';
import { Client } from 'pg';

// Default MongoDB and PostgreSQL connection parameters
const DEFAULT_MONGO_URI = "mongodb://localhost:27017";
const DEFAULT_POSTGRES_HOST = "localhost";
const DEFAULT_POSTGRES_DB = "fruitdb";
const DEFAULT_POSTGRES_USER = "postgres";
const DEFAULT_POSTGRES_PASSWORD = "password";

export async function handler(event) {
    let mongoUri = process.env.MONGO_URI || DEFAULT_MONGO_URI;
    let postgresHost = process.env.POSTGRES_HOST || DEFAULT_POSTGRES_HOST;
    let postgresDb = process.env.POSTGRES_DB || DEFAULT_POSTGRES_DB;
    let postgresUser = process.env.POSTGRES_USER || DEFAULT_POSTGRES_USER;
    let postgresPassword = process.env.POSTGRES_PASSWORD || DEFAULT_POSTGRES_PASSWORD;

    // Check if the Lambda input (event) contains overrides
    if (event && event.mongoUri) mongoUri = event.mongoUri;
    if (event && event.postgresHost) postgresHost = event.postgresHost;
    if (event && event.postgresDb) postgresDb = event.postgresDb;
    if (event && event.postgresUser) postgresUser = event.postgresUser;
    if (event && event.postgresPassword) postgresPassword = event.postgresPassword;

    try {
        // Extract data from MongoDB
        const client = new MongoClient(mongoUri);
        try {
            await client.connect();
        }catch (error){
            console.error('Error connecting to MongoDB: ', error);
        }


        const database = client.db('fruitdb');
        const fruitsCollection = database.collection('fruits');
        const fruits = await fruitsCollection.find({}).toArray();
        console.log('Extracted Fruits Data:', fruits);

        
        
        // Transform portion.. can split the data, reformat or roll up
        const transformedFruits = fruits.map(fruit => ({
            name: fruit.name
        }));
        console.log('Transformed Fruits Data:', transformedFruits);



        // Load data into PostgreSQL
        const pgClient = new Client({
            host: postgresHost,
            database: postgresDb,
            user: postgresUser,
            password: postgresPassword,
        });
        try {
            await pgClient.connect();
        }catch (error){
            console.error('Error connecting to MongoDB: ', error);
        }
        

        const insertQuery = 'INSERT INTO fruit_table(name) VALUES($1)';
        const insertPromises = transformedFruits.map(fruit => {
            return pgClient.query(insertQuery, [fruit.name]);
        });

        await Promise.all(insertPromises);
        console.log('Data loaded into PostgreSQL successfully.');

        // Close PostgreSQL connection
        await pgClient.end();
        await client.close();

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "ETL process completed successfully.",
                input: event,
            }),
        };

    } catch (error) {
        console.error("Error occurred during ETL process:", error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: "ETL process failed.",
                error: error.message,
            }),
        };
    }
}