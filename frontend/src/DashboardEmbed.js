import React from 'react';

const DashboardEmbed = () => {
  return (
    <div className="dashboard-container">
      <h2>Інтерактивний Дашборд</h2>
      
      {/* Це твій новий код з Tableau Cloud. 
        Я просто змінив 'width' і 'height' для кращої
        адаптивності, як ми робили раніше.
      */}
      <tableau-viz 
        id='tableau-viz' 
        src='https://dub01.online.tableau.com/t/erikgriniov1-3be6b7b2f9/views/Diploma/Dashboard1' 
        width='100%' 
        height='827' 
        hide-tabs 
        toolbar='bottom'
      >
      </tableau-viz>
      
    </div>
  );
};

export default DashboardEmbed;