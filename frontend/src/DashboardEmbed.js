import React from 'react';

// 1. Приймаємо "ключ оновлення"
const DashboardEmbed = ({ refreshKey }) => {

  // 2. Це наше базове посилання на дашборд
  const baseUrl = 'https://dub01.online.tableau.com/t/erikgriniov1-3be6b7b2f9/views/Diploma/Dashboard1?:embed=y&:showVizHome=no&:toolbar=no&:tabs=no';

  // 3. Ми "обманюємо" iframe, додаючи до URL фіктивний параметр,
  // який змінюється. Коли URL змінюється, iframe ПРИМУСОВО
  // перезавантажується і завантажує НОВІ дані.
  const tableauEmbedUrl = refreshKey 
    ? `${baseUrl}&:refresh_key=${refreshKey}` 
    : baseUrl;

  return (
    <div className="dashboard-container">
      <h2>Інтерактивний Дашборд</h2>

      <iframe
        src={tableauEmbedUrl} // <-- Використовуємо наш новий URL
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