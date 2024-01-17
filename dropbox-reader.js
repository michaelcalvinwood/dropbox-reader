const mysql = require('./utils/mysql');
const { insertDocument } = require('./utils/mongo');
const { createReadStream } = require('fs');
const { createTempFile } = require('./utils/file');
const { addTopics } = require('./connected-economy');
const { parseCsv, getUniversalTablesData } = require('./parse');
const { getList, getFileContent, getProjectFiles } = require('./dropbox');
const deepGet = require('lodash.get');
const { toCsv } = require('./utils/csv');

const createUpsertTopicProcedure = async () => {
  try {
    const checkProcedureSQL = `
      SELECT ROUTINE_NAME
      FROM INFORMATION_SCHEMA.ROUTINES
      WHERE ROUTINE_NAME = 'UpsertTopic'
      AND ROUTINE_SCHEMA = DATABASE();
    `;

    const result = await mysql.poolQuery(checkProcedureSQL);

    if (!result.length) {
      const createProcedureSQL = `
        CREATE PROCEDURE UpsertTopic(
          topic_name VARCHAR(255), json_value VARCHAR(2048)
        )
        BEGIN
            DECLARE topic_id INT;

            SELECT id INTO topic_id
            FROM topic
            WHERE topic = topic_name
            LIMIT 1;

            IF topic_id IS NOT NULL THEN
                IF JSON_CONTAINS(
                  (SELECT tables FROM topic WHERE id = topic_id),
                  JSON_QUOTE(json_value)
                ) = 0 THEN
                    UPDATE topic t
                    SET t.tables = JSON_ARRAY_APPEND(
                        t.tables,
                        '$',
                        JSON_UNQUOTE(JSON_QUOTE(json_value))
                    )
                    WHERE t.id = topic_id;
                END IF;
            ELSE
                INSERT INTO topic (topic, tables)
                VALUES (topic_name, JSON_ARRAY(json_value));
            END IF;
        END;
      `;

      await mysql.poolQuery(createProcedureSQL);
    }
  } catch (error) {
    console.error('Error:', error);
  }
};

const generateUniversalTablesSql = schema => {
  const queries = schema.map(({ tableName, values }) =>
    values.map(item => {
      if (tableName === 'topic') {
        return `CALL UpsertTopic(${item.topic}, ${item.project});`;
      }

      const columns = Object.keys(item);

      const vals = Object.values(item).join(', ');

      const pairs = Object.entries(item)
        .map(([key, value]) => `${key} = ${value}`)
        .join(' AND ');

      return `INSERT IGNORE INTO ${tableName} (${columns})
              SELECT ${vals}
              FROM dual
              WHERE NOT EXISTS (
                  SELECT 1
                  FROM ${tableName}
                  WHERE ${pairs}
                  LIMIT 1
              );`;
    })
  );

  return queries.flat();
};

const generateFindIdsSql = item => {
  const schema = [
    {
      tableName: 'timeframe',
      name: 'edition_id',
      columns: {
        edition: 'edition',
        project: 'project'
      }
    },
    {
      tableName: 'location',
      name: 'location_id',
      columns: {
        country: 'name'
      }
    },
    {
      tableName: 'question',
      name: 'question_id',
      columns: {
        code: 'code',
        project: 'project'
      }
    },
    {
      tableName: 'answer',
      name: 'answer_id',
      columns: {
        answer: 'answer'
      }
    },
    {
      tableName: 'demo',
      name: 'demo_id',
      columns: {
        demo: 'demo'
      }
    },
    {
      tableName: 'demo_cut',
      name: 'demo_cut_id',
      columns: {
        demo_cut: 'demo_cut'
      }
    },
    ...item.topics.map((_, i) => ({
      tableName: 'topic',
      name: `topic_${i + 1}`,
      columns: {
        [`topics[${i}].topic`]: 'topic'
      }
    })),
    ...item.topics.map((_, i) => ({
      tableName: 'action',
      name: `action_${i + 1}`,
      columns: {
        [`topics[${i}].action`]: 'action',
        [`topics[${i}].topic`]: 'topic_id'
      }
    })),
    ...item.topics.map((_, i) => ({
      tableName: 'label',
      name: `E${i + 1}_label_A`,
      columns: {
        [`topics[${i}].label_A`]: 'label'
      }
    })),
    ...item.topics.map((_, i) => ({
      tableName: 'label',
      name: `E${i + 1}_label_B`,
      columns: {
        [`topics[${i}].label_B`]: 'label'
      }
    })),
    ...item.topics.map((_, i) => ({
      tableName: 'label',
      name: `E${i + 1}_label_C`,
      columns: {
        [`topics[${i}].label_C`]: 'label'
      }
    }))
  ];

  const queries = schema.reduce((acc, { tableName, columns, name }) => {
    const conditions = Object.entries(columns)
      .map(([key, column]) => {
        const name = column.replace('_id', '');

        const value = deepGet(item, key);

        return `${column} = ${
          ['topic_id', 'demo_cut_id'].includes(column)
            ? `(SELECT id FROM ${name} WHERE ${name} = "${value}" LIMIT 1)`
            : `"${value}"`
        }`;
      })
      .join(' AND ');

    return {
      ...acc,
      [name]: `SELECT id FROM ${tableName} WHERE ${conditions} LIMIT 1`
    };
  }, {});

  return queries;
};

const seedUniversalTables = async data => {
  await createUpsertTopicProcedure();

  const queries = generateUniversalTablesSql(getUniversalTablesData(data));

  for (let query of queries) {
    try {
      await mysql.poolQuery(query);
    } catch (error) {
      console.error(error);
    }
  }
};

const formatReading = item => {
  const schema = {
    country: item.country,
    fantasy_edition: item.fantasy_edition,
    demo_cut: item.demo_cut === 'sample' ? 'all' : item.demo_cut
  };

  return Object.entries(schema).reduce(
    (acc, [key, value]) => acc.replace(`[${key}]`, value),
    item.reading
  );
};

const getForeignKeys = async item => {
  const queries = generateFindIdsSql(item);

  const result = await Promise.allSettled(
    Object.values(queries).map(query => mysql.poolQuery(query))
  );

  const keys = Object.keys(queries);

  return result
    .filter(({ status }) => status === 'fulfilled')
    .reduce(
      (acc, { value }, i) => ({
        ...acc,
        [keys[i]]: value[0]?.id
      }),
      {}
    );
};

const writeProjectTable = async (values, project, edition) => {
  try {
    const { header, csv } = toCsv(values);

    const csvFile = createTempFile(csv, [project, edition].join('-') + '.csv');

    const query = `LOAD DATA LOCAL INFILE '${csvFile}' INTO TABLE ${project} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES (${header});`;

    await mysql.poolQuery(query, {
      infileStreamFactory: () => createReadStream(csvFile)
    });
  } catch (error) {
    console.error(error);
  }
};

const getProjectTableData = async data => {
  const values = [];

  for (let item of data) {
    try {
      const foreignKeys = await getForeignKeys(item);

      const row = {
        ...foreignKeys,
        fielding: item.fielding,
        chart: parseInt(item.chart),
        reading: formatReading(item),
        subset_text: item.subset_text,
        float_value: parseFloat(item.value),
        integer_value: parseInt(item.value),
        value_type: parseInt(item.value_format),
        subset_count: parseInt(item.subset_count)
      };

      values.push(row);
    } catch (error) {
      console.error(error);
    }
  }

  return values;
};

const seedProjectTable = async data => {
  const edition = data[0].edition;

  const project = data[0].project.toLowerCase().replace(/\s+/g, '_');

  if (!['connected_economy', 'lending_club'].includes(project)) {
    return;
  }

  const values = await getProjectTableData(data);

  await writeProjectTable(values, project, edition);
};

const seedMetadata = async json => {
  if (!json?.id) {
    return;
  }

  try {
    const metadata = await getFileContent(json.id);

    await insertDocument(metadata.csv, metadata);
  } catch (error) {
    console.error("Can't add metadata for file:", json);
  }
};

const isConnectedEconomy = ({ path, name }) =>
  path.startsWith('/DataVizFolder/connectedEconomy/') &&
  /CE(\d){6}\.(csv|json)/.test(name);

const isLendingClub = ({ path, name }) =>
  path.startsWith('/DataVizFolder/LC/') && /LC(\d){6}\.(csv|json)/.test(name);

const shouldSkipFile = async ([{ edition, project }]) => {
  const result = await mysql.poolQuery(`
        SELECT id
          FROM timeframe
          WHERE edition = "${edition}"
          AND project = "${project}"
          LIMIT 1
      `);

  return !!result.length;
};

const main = async () => {
  await getList();

  const projectFiles = getProjectFiles(
    file => isConnectedEconomy(file) || isLendingClub(file)
  );

  for (let i = 0; i < projectFiles.length; i++) {
    const { csv, json } = projectFiles[i];

    const rawContent = await getFileContent(csv.id);

    const { data } = parseCsv(csv, rawContent);

    if (await shouldSkipFile(data)) {
      console.log(
        '=============== Skiped ================>>>',
        i + 1,
        csv.name
      );

      continue;
    }

    console.log('============= Not Skiped ==============>>>', i + 1, csv.name);

    await seedMetadata(json);

    console.log(
      '============== Metadata ===============>>>',
      i + 1,
      json?.name
    );

    const dataWithTopics = addTopics(data);

    await seedUniversalTables(dataWithTopics);

    console.log('========== Universal tables ===========>>>', i + 1, csv.name);

    await seedProjectTable(dataWithTopics);

    console.log('==========  Project tables  ===========>>>', i + 1, csv.name);
  }

  process.exit(0);
};

main();
