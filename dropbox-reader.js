/*
 * Endpoints: https://www.dropbox.com/developers/documentation/http/documentation
 */

require('dotenv').config();
// const mysql = require('./utils/mysql');

const { parse } = require('csv-parse/sync');

const axios = require('axios');

const { DROPBOX_TOKEN } = process.env;

/*
 * Global variables
 */

const list = []; // list of all files
const csv = []; // list of csv files

const parseCsv = (file, content) => {
  const data = parse(content, {
    delimiter: '|',
    columns: true,
    skip_empty_lines: true
  });

  return {
    ...file,
    data
  };
};

const getCsvContent = async path => {
  const request = {
    url: 'https://content.dropboxapi.com/2/files/download',
    method: 'post',
    headers: {
      Authorization: `Bearer ${DROPBOX_TOKEN}`,
      'Content-Type': 'text/plain',
      'Dropbox-API-Arg': JSON.stringify({ path })
    }
  };

  try {
    response = await axios(request);

    return response.data;
  } catch (err) {
    console.error(err.response);
    return false;
  }
};

const listContinue = async cursor => {
  const request = {
    url: 'https://api.dropboxapi.com/2/files/list_folder/continue',
    method: 'post',
    headers: {
      Authorization: `Bearer ${DROPBOX_TOKEN}`,
      'Content-Type': 'application/json'
    },
    data: {
      cursor
    }
  };

  try {
    response = await axios(request);

    return response.data;
  } catch (err) {
    console.error(err);
    return false;
  }
};

const listFiles = async path => {
  const request = {
    url: 'https://api.dropboxapi.com/2/files/list_folder',
    method: 'post',
    headers: {
      Authorization: `Bearer ${DROPBOX_TOKEN}`,
      'Content-Type': 'application/json'
    },
    data: {
      include_deleted: false,
      include_has_explicit_shared_members: false,
      include_media_info: false,
      include_mounted_folders: true,
      include_non_downloadable_files: true,
      recursive: true,
      path
    }
  };

  try {
    response = await axios(request);
    return response.data;
  } catch (err) {
    console.error(err);
    return false;
  }
};

const processFiles = files => {
  const { entries } = files;

  entries.forEach(entry => {
    const tag = entry['.tag'];
    if (tag === 'file') {
      const { name, path_display, client_modified, rev, id } = entry;
      list.push({
        name,
        path: path_display,
        modified: client_modified,
        rev,
        id
      });
    }
  });
};

const getList = async () => {
  let files = await listFiles('/DataVizFolder/');
  processFiles(files);
  let { has_more } = files;
  let { cursor } = files;
  while (has_more) {
    let files = await listContinue(cursor);
    processFiles(files);
    has_more = files.has_more;
    cursor = files.cursor;
  }
};

const getCsv = () => {
  for (let i = 0; i < list.length; ++i) {
    if (list[i].path.endsWith('.csv')) csv.push(list[i]);
  }
};

const loadSQL = async () => {
  await getList();

  getCsv();

  for (const file of csv) {
    const rawContent = await getCsvContent(file.id);

    const content = parseCsv(file, rawContent);

    console.log(JSON.stringify(content, null, 2));

    break;
  }
};

loadSQL();
