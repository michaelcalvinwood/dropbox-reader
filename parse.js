const { parse } = require('csv-parse/sync');
const { getStartDate, getEndDate } = require('./utils/date');

const findDistinctValues = (data, keys, options = {}) => {
  const distinctMap = new Map();

  const { columns, delimiter, extraFields } = {
    columns: {},
    extraFields: [],
    delimiter: '<<>>',
    ...options
  };

  const formatItem = item => ({
    ...keys.reduce(
      (acc, key) => ({
        ...acc,
        [columns[key] ?? key]: `"${item[key]}"`
      }),
      {}
    ),
    ...extraFields.reduce((acc, { name, getValue }) => {
      return {
        ...acc,
        [name]: getValue(item)
      };
    }, {})
  });

  for (const obj of data) {
    const keyValues = keys.map(key => obj[key]).join(delimiter);

    if (!distinctMap.has(keyValues)) {
      distinctMap.set(keyValues, obj);
    }
  }

  return [...distinctMap.values()].map(formatItem);
};

const getTopicsTableData = data => ({
  tableName: 'topic',
  values: [
    ...new Set(
      data.map(({ topics }) => topics.map(({ topic }) => topic)).flat()
    )
  ].map(topic => ({
    topic: `"${topic}"`,
    project: `"${data[0].project.toLowerCase().replace(/\s/g, '_')}"`
  }))
});

const getLabelsTableData = data => ({
  tableName: 'label',
  values: [
    ...new Set(
      data
        .map(({ topics }) =>
          topics
            .map(({ label_A, label_B, label_C }) =>
              [label_A, label_B, label_C].filter(Boolean)
            )
            .flat()
        )
        .flat()
    )
  ].map(label => ({ label: `"${label}"` }))
});

const getActionsTableData = data => ({
  tableName: 'action',
  values: [
    ...new Set(
      data
        .map(({ topics }) =>
          topics.map(({ topic, action }) => [topic, action].join('<<>>'))
        )
        .flat()
    )
  ].map(item => {
    const [topic, action] = item.split('<<>>');

    return {
      action: `"${action}"`,
      topic_id: `(SELECT id FROM topic WHERE topic = "${topic}")`
    };
  })
});

exports.getUniversalTablesData = data => {
  const schema = [
    {
      tableName: 'location',
      keys: ['country'],
      columns: { country: 'name' }
    },
    {
      tableName: 'demo',
      keys: ['demo', 'fantasy_demo'],
      columns: {
        fantasy_demo: 'demo_fantasy'
      }
    },
    {
      tableName: 'question',
      keys: ['code', 'question', 'fantasy_question', 'project']
    },
    {
      tableName: 'answer',
      keys: ['answer', 'fantasy_answer']
    },
    {
      tableName: 'timeframe',
      keys: ['project', 'edition'],
      extraFields: [
        {
          name: 'start',
          getValue: item => `"${getStartDate(item.edition)}"`
        },
        {
          name: 'end',
          getValue: item => `"${getEndDate(item.edition)}"`
        }
      ]
    },
    {
      tableName: 'demo_cut',
      keys: ['demo_cut', 'fantasy_demo_cut'],
      columns: {
        fantasy_demo_cut: 'demo_cut_fantasy'
      },
      extraFields: [
        {
          name: 'demo_id',
          getValue: item => `(SELECT id FROM demo WHERE demo = "${item.demo}")`
        }
      ]
    }
  ];

  return [
    getLabelsTableData(data),
    getTopicsTableData(data),
    getActionsTableData(data),
    ...schema.map(({ tableName, keys, ...options }) => ({
      tableName,
      values: findDistinctValues(data, keys, options)
    }))
  ];
};

exports.parseCsv = (file, content) => {
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
