Все файлы в директории `memory/` автоматически подгружаются как контекст.  
**Правило:**  
- Amazon Q при каждом запросе учитает содержимое этих файлов — чтобы “не забывать” предыдущую логику и данные.  
- Filewatch: любой `.md`, `.txt`, `.json` в `memory/` попадает в чат-контекст.
- Можно использовать /context hooks add git-status --trigger per_prompt --command "git status --short" /context hooks add project-info --trigger conversation_start --command "echo 'Project: '$(basename $(pwd))"