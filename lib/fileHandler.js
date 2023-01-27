const { appendFile, mkdir } = require('fs/promises');
const { existsSync } = require('fs');
const path = require('path')
async function appendToFile(fileName, data) {
  try {
    // if (! await existsSync(path.resolve(dirName, fileName))) {
    //   await mkdir(path.dirname(dirName))
    // }
    await appendFile(fileName, data, { flag: 'a' });
    console.log(`Appended data to ${fileName}`);
  } catch (error) {
    console.error(`Got an error trying to append the file: ${error.message}`);
  }
}

module.exports = {
  appendToFile
}