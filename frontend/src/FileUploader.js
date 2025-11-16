import React, { useState } from 'react';
import axios from 'axios'; // Імпортуємо axios

function FileUploader() {
  const [selectedFile, setSelectedFile] = useState(null);
  const [message, setMessage] = useState(''); // Для сповіщень
  const [insights, setInsights] = useState([]);

  // Обробник вибору файлу
  const onFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
    setMessage('');
    setInsights([]);
  };

  // Обробник завантаження файлу
  const onFileUpload = () => {
    if (!selectedFile) {
      setMessage('Помилка: Файл не обрано!');
      return;
    }

    // Створюємо FormData для надсилання файлу
    const formData = new FormData();
    formData.append('file', selectedFile);

    setMessage('Завантаження та обробка...');

    // Надсилаємо запит на наш Flask Backend (який працює на порті 5000)
    axios.post('https://my-diploma-project.onrender.com', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })
    .then((response) => {
      // Успіх
      console.log(response.data);
      setMessage(`Успіх: ${response.data.message}`);

      // Зберігаємо інсайти, які прийшли з бекенду
      setInsights(response.data.insights || []);
    })
    .catch((error) => {
      // Помилка
      console.error('Помилка завантаження:', error);
      setMessage(`Помилка: ${error.response ? error.response.data.error : 'Сервер не відповідає'}`);
    });
  };

  return (
  <div className="uploader-container">
    <h2>Завантажте CSV-файл для аналізу</h2>
    <p>Система автоматично розпізнає стовпці та оновить дашборд.</p>

    {/* Замінюємо старий input/button на це: */}
    <div>
      <label htmlFor="file-upload" className="custom-file-upload">
        Обрати файл
      </label>
      <input id="file-upload" type="file" onChange={onFileChange} accept=".csv" />

      <button 
        onClick={onFileUpload} 
        className="upload-button"
        disabled={!selectedFile}
      >
        Завантажити та Обробити
      </button>

      {selectedFile && <span className="file-name">{selectedFile.name}</span>}
    </div>

    {/* Показуємо повідомлення про статус */}
    {message && <p className="message">{message}</p>}

    {/* Блок для відображення інсайтів */}
  {insights.length > 0 && (
    <div className="insights-container">
      <h3>💡 Розумні Рекомендації</h3>
      <ul>
        {insights.map((insight, index) => (
          <li key={index}>{insight}</li>
        ))}
      </ul>
    </div>
  )}
  </div>
);
}

export default FileUploader;