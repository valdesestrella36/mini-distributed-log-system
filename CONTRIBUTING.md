## Contributing

Gracias por tu interés en contribuir a mini-distributed-log-system. Sigue estos pasos para hacer cambios:

- Forkea el repositorio y crea una rama para tu feature o fix: `git checkout -b feature/nombre`
- Asegúrate de que tu rama esté actualizada con la rama `main` del upstream.
- Escribe tests para cambios relevantes y asegúrate de que `pytest` pase.

Setup local (recomendado):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r broker/requirements-dev.txt
```

Ejecutar tests:

```bash
pytest -q broker/tests
```

Formato de contribución:
- Mantén los commits pequeños y con mensajes descriptivos.
- Crea un Pull Request hacia `main` y describe el objetivo del cambio.
- Responde a comentarios y añade pruebas si es necesario.

Estilo y calidad:
- Recomendamos usar `black` para formateo y `flake8` para linting (configurado en `broker/requirements-dev.txt`).

Reportar problemas:
- Abre un issue con un título claro y pasos para reproducir.

¡Gracias por contribuir!
