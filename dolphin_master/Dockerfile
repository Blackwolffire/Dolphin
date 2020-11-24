FROM python:3
COPY ./ .
WORKDIR /
RUN pip3 install -r requirements.txt --no-cache-dir --compile --global-option=build_ext
CMD ["python", "/dolphin_master/worker.py"]
