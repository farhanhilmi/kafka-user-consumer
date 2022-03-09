import dotenv from 'dotenv';

dotenv.config();

// eslint-disable-next-line object-curly-newline
const { MONGODB_URI, PORT } = process.env;

const config = {
  app: {
    port: PORT || 8000,
  },
  db: {
    uri: MONGODB_URI,
  },
};

export default config;
