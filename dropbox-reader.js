/*
 * Endpoints: https://www.dropbox.com/developers/documentation/http/documentation
 */

require('dotenv').config()
const mysql = require('./utils/mysql')

const axios = require('axios');

const { DROPBOX_TOKEN } = process.env;

/*
 * Global variables
 */

const list = []; // list of all files
const csv = []; // list of csv files


const getFileUrl = async (id) => {
    if (id.startsWith('id:')) id = id.substring(3);
    console.log('id:', id);
    //return;
    const request = {
        url: 'https://api.dropboxapi.com/2/file_requests/get',
        method: 'post',
        headers: {
            "Authorization": `Bearer ${DROPBOX_TOKEN}`,
            "Content-Type": "application/json"
        },
        data: {
           id
        }
    }

    try {
        response = await axios(request);
        return(response.data);
    } catch(err) {
        console.error(err.response.data);
        return false;
    }
}

const listContinue = async (cursor) => {
    const request = {
        url: 'https://api.dropboxapi.com/2/files/list_folder/continue',
        method: 'post',
        headers: {
            "Authorization": `Bearer ${DROPBOX_TOKEN}`,
            "Content-Type": "application/json"
        },
        data: {
            cursor
        }
    }

    try {
        response = await axios(request);
        return response.data
    } catch(err) {
        console.error(err);
        return false;
    }
}

const listFiles = async (path) => {
    const request = {
        url: 'https://api.dropboxapi.com/2/files/list_folder',
        method: 'post',
        headers: {
            "Authorization": `Bearer ${DROPBOX_TOKEN}`,
            "Content-Type": "application/json"
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
    }

    try {
        response = await axios(request);
        return response.data
    } catch(err) {
        console.error(err);
        return false;
    }
}


const processFiles = (files) => {
    const { entries } = files;
    entries.forEach(entry => {
        const tag = entry['.tag'];
        if (tag === 'file') {
            const { name, path_display, client_modified, rev, id } = entry;
            list.push({
                name, path: path_display, modified: client_modified, rev, id
            })
        }
    })
}

const getList = async () => {
    let files = await listFiles('/DataVizFolder/');
    processFiles(files);
    let { has_more } = files;
    let { cursor } = files;
    while (has_more) {
        let files = await listContinue(cursor);
        processFiles(files);
        has_more = files.has_more;
        cursor = files.cursor
    }
}

const getCsv = () => {
    for (let i = 0; i < list.length; ++i) {
        if (list[i].path.endsWith('.csv')) csv.push(list[i])
    }
}

const loadSQL = async () => {
    await getList();
    getCsv();
    console.log(csv[0])
    const fileUrl = await getFileUrl(csv[1].id)
    console.log(fileUrl)
}

loadSQL();
