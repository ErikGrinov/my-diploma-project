import React from 'react';
import './App.css';
import FileUploader from './FileUploader';
import DashboardEmbed from './DashboardEmbed';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        
        
        <div className="watermark">
          ©Hrynov Erik CHERNIVTSI NATIONAL UNIVERSITY
        </div>
 

        <h1> Інформаційна система аналізу продажів з використанням бізнес-аналітики</h1>
        <p>Кваліфікаційна робота</p>
      </header>
      
      <main>
        {/* Компонент для завантаження файлу */}
        <FileUploader />
        
        {/* Компонент для відображення дашборда */}
        <DashboardEmbed />
      </main>
    </div>
  );
}

export default App;