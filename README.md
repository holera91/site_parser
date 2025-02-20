# parser

На Windows установку зависимостей из `requirements.txt` в виртуальное окружение можно выполнить так:

### 1. Создай виртуальное окружение (если его еще нет)

Открой терминал (PowerShell или `cmd`) и перейди в папку с проектом:

```sh
python -m venv venv
```

Это создаст папку `venv` с виртуальным окружением.

### 2. Активируй виртуальное окружение

- В `cmd`:
  ```sh
  venv\Scripts\activate
  ```
- В PowerShell:
  ```sh
  venv\Scripts\Activate.ps1
  ```
  **Важно:** Если PowerShell блокирует выполнение скриптов, можно разрешить их командой:
  ```sh
  Set-ExecutionPolicy Unrestricted -Scope Process
  ```

### 3. Установи зависимости из `requirements.txt`

После активации окружения выполни:

```sh
pip install -r requirements.txt
```

Готово! Теперь все зависимости установлены в виртуальное окружение. 🚀
