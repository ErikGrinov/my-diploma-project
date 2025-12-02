import React from 'react';

const DashboardEmbed = ({ refreshKey }) => {

  // 1. Базове посилання (БЕЗ знаків питання в кінці)
  const baseUrl = 'https://dub01.online.tableau.com/t/hrynoverik-dfdbf96f2e/views/Diploma/Dashboard1';

  // 2. Формуємо "Ядерний URL"
  //    ?:embed=yes       -> Вмикає режим вбудовування
  //    &:refresh=yes     -> НАКАЗУЄ Tableau ігнорувати кеш і взяти свіжі дані
  //    &:ts=${refreshKey} -> НАКАЗУЄ Браузеру вважати це новою сторінкою (timestamp)
  
  // Використовуємо поточний час, якщо refreshKey ще немає
  const currentTimestamp = refreshKey || new Date().getTime();
  
  const finalUrl = `${baseUrl}?:embed=yes&:showVizHome=no&:toolbar=no&:tabs=no&:refresh=yes&:ts=${currentTimestamp}`;

  console.log("Завантажую дашборд з URL:", finalUrl);

  return (
    <div className="dashboard-container">
      <h2>Інтерактивний Дашборд</h2>

      <iframe
        src={finalUrl}
        width="100%"
        height="827"
        frameBorder="0"
        allowFullScreen
        title="Tableau Dashboard"
      ></iframe>

    </div>
  );
};

export default DashboardEmbed;