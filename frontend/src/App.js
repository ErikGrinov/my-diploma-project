import React, { useState } from 'react'; // <-- Додаємо useState
import './App.css';
import FileUploader from './FileUploader';
import DashboardEmbed from './DashboardEmbed';

function App() {
  // 1. Додаємо "ключ оновлення"
  const [refreshKey, setRefreshKey] = useState(null);

  // 2. Функція, яка буде викликатися після успішного завантаження
  const handleUploadSuccess = () => {
    console.log("App.js: Отримано сигнал! Запускаю оновлення дашборда...");
    // 3. Змінюємо ключ, щоб React "побачив" зміни
    setRefreshKey(new Date().getTime()); 
  };

  return (
    <div className="App">
      <header className="App-header">
        <div className="watermark">
          © Hrynov Erik CHERNIVTSI NATIONAL UNIVERSITY
        </div>
        <h1>Інформаційна система аналізу продажів</h1>
        <p>Кваліфікаційна робота</p>
      </header>
      
      <main>
        {/* 4. Передаємо функцію у FileUploader */}
        <FileUploader onUploadSuccess={handleUploadSuccess} />
        
        {/* 5. Передаємо ключ у DashboardEmbed */}
        <DashboardEmbed refreshKey={refreshKey} />
      </main>
    </div>
  );
}

export default App;