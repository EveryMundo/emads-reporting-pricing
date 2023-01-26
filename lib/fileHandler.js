const { appendFile } = require('fs/promises');

async function appendToFile(fileName, data) {
  try {
    await appendFile(fileName, data, { flag: 'a' });
    console.log(`Appended data to ${fileName}`);
  } catch (error) {
    console.error(`Got an error trying to append the file: {error.message}`);
  }
}

module.exports = {
  appendToFile
}