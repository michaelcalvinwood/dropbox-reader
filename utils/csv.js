exports.toCsv = data => {
  const allKeys = Array.from(
    new Set(data.reduce((keys, obj) => keys.concat(Object.keys(obj)), []))
  );

  const header = allKeys.join(',') + '\n';

  const csv = data
    .map(obj =>
      allKeys
        .map(key => {
          const value = obj[key];

          return value !== undefined && typeof value !== 'number'
            ? `"${value}"`
            : value !== undefined
            ? value
            : 'NULL';
        })
        .join(',')
    )
    .join('\n');

  return {
    header,
    data: csv,
    csv: header + csv
  };
};
