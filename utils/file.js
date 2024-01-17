const { writeFileSync } = require('fs');
const { tmpdir } = require('os');
const { join } = require('path');

exports.createTempFile = (data, tempFileName) => {
  try {
    const tempFilePath = join(tmpdir(), tempFileName);

    writeFileSync(tempFilePath, data);

    return tempFilePath;
  } catch (error) {
    console.error('Error creating temporary file:', error);

    return null;
  }
};
