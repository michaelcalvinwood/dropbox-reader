exports.addTopics = data =>
  data.map(item => ({
    ...item,
    topics: new Array(5)
      .fill(1)
      .map((val, i) => {
        const number = val + i;

        if (!item[`topic_${number}`]) {
          return null;
        }

        const result = {
          topic: item[`topic_${number}`],
          action: item[`data_point_${number}`]
        };

        for (let char of ['A', 'B', 'C']) {
          const label = item[`E${number}_label_${char}`];

          if (label) {
            result[`label_${char}`] = label;
          }
        }

        return result;
      })
      .filter(Boolean)
  }));
