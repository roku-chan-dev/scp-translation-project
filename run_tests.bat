@echo off
echo Running pytest for SCP Translation Project...
python -m pytest -v
if %ERRORLEVEL% EQU 0 (
    echo All tests passed!
) else (
    echo Tests failed with error code %ERRORLEVEL%
)
pause