const createDateFunctions = format => {
  const regex = /^(\d{4})(\d{2})$/;

  const extractYearMonth = input => {
    const match = input.match(regex);

    if (!match) {
      throw new Error(`Invalid input format. Please use '${format}' format.`);
    }

    const year = parseInt(match[1], 10);

    const month = parseInt(match[2], 10);

    return { year, month };
  };

  const getStartDate = input => {
    const { year, month } = extractYearMonth(input);

    const startDate = new Date(year, month - 1, 1);

    return startDate.toISOString().slice(0, 10);
  };

  const getEndDate = input => {
    const { year, month } = extractYearMonth(input);

    const endDate = new Date(year, month, 0);

    return endDate.toISOString().slice(0, 10);
  };

  return {
    getStartDate,
    getEndDate
  };
};

module.exports = createDateFunctions('YYYYMM');
