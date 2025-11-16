import React from 'react';

// Це посилання містить параметр :embed=y, який часто вирішує проблему
const tableauEmbedUrl = 'https://public.tableau.com/views/Diploma_17632298514930/Dashboard1?:embed=y&:showVizHome=no&:display_count=no';

const DashboardEmbed = () => {
  return (
    <div className="dashboard-container">
      <h2>Інтерактивний Дашборд</h2>

      <iframe
        src={tableauEmbedUrl}
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