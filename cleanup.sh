#!/bin/bash
# Скрипт для очистки проекта от лишних файлов

echo "Очистка проекта от лишних файлов..."

# Список файлов для удаления
FILES_TO_REMOVE=(
  "apply_client_fix.py"
  "apply_command_fix.py"
  "apply_flexible_command.py"
  "apply_listener_fix.py"
  "apply_listener_terminator_fix.py"
  "apply_listener_update.py"
  "apply_patch.py"
  "apply_terminator_fixes.py"
  "patch_config.py"
  "patch_main.py"
  "patch_shuttle_listener.py"
  "save_shuttle.py"
  "shuttle_module/commands_fixed.py"
  "shuttle_module/commands_flexible.py"
  "shuttle_module/shuttle_client_fixed.py"
  "shuttle_module/shuttle_client_simple.py"
  "shuttle_module/shuttle_client_v2.py"
  "shuttle_module/shuttle_listener_fixed.py"
  "shuttle_module/shuttle_listener_terminator_fix.py"
  "shuttle_module/shuttle_listener_updated.py"
  "shuttle_module/shuttle_listener_v2.py"
  "shuttle_module/shuttle_manager_patch.py"
  "shuttle_module/shuttle_manager_simple.py"
  "shuttle_module/shuttle_manager_v2.py"
  "test_binary_terminators.py"
  "test_command_fixed.py"
  "test_command_formats.py"
  "test_encodings.py"
  "test_flexible_command.py"
  "test_formats.py"
  "test_listener_terminators.py"
  "test_shuttle_full.py"
  "test_shuttle_simple.py"
  "test_shuttle_v2.py"
  "test_terminators.py"
  "test_wms_integration.py"
  "run_fixed.sh"
  "run_simple.sh"
  "run_v2.sh"
  "run_with_check.sh"
  "run_with_save.sh"
)

# Удаляем файлы, если они существуют
for file in "${FILES_TO_REMOVE[@]}"; do
  if [ -f "$file" ]; then
    echo "Удаление файла: $file"
    rm "$file"
  fi
done

# Удаляем резервные копии
find . -name "*.bak" -type f -delete
find . -name "*.old" -type f -delete

echo "Очистка завершена"