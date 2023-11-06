import threading

import colorlog
import redis
import logging


logger = colorlog.getLogger()
logger.setLevel(logging.INFO)
# Встановлюємо колір для всіх рівнів логів
log_colors = {
    'DEBUG': 'green',
    'INFO': 'green',
    'WARNING': 'green',
    'ERROR': 'green',
    'CRITICAL': 'green',
}
# Встановлюємо формат для логів
formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)s:%(name)s:%(message)s",
    log_colors=log_colors,
)
# Створюємо обробник для виведення логів на консоль
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)


# Додаємо обробник до логгера
logger.addHandler(console_handler)


# Підключення до сервера Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)
pipe = r.pipeline()
pipe.multi()
# Додаємо операцію GET до транзакції
pipe.get('consumer')
# Додаємо операцію INCR до транзакції
pipe.incr('count')


# Виконуємо транзакцію і отримуємо результати
# result = pipe.execute()

# Виводимо результати
# print(result)
# Процес No 1: Дозапис даних в блоковану чергу
def process_1(data_to_enqueue):
    logger = logging.getLogger("Process 1")

    # Додаємо дані до черги
    r.rpush("feedback", data_to_enqueue)
    logger.info(f"Додано дані до черги: {data_to_enqueue}")

# Процес No 2: Зчитування даних з черги і запис у файл
def process_2():
    logger = logging.getLogger("Process 2")

    while True:
        data = r.blpop("feedback", timeout=3)
        if data:
            data = data[1].decode("utf-8")
            logger.info(f"Зчитано дані з черги: {data}")

            # Записуємо дані у текстовий файл
            with open("output.txt", "a") as file:
                file.write(data + "\n")
                logger.info(f"Записано дані у файл: {data}")
        else:
            logger.info('Черга порожня. Завершення роботи.')
            break


# Викликаємо процес No 1 для додавання даних до черги
process_1("Good item")
process_1("Cool quality")

# Запускаємо процес No 2 у окремому потоці
if __name__ == '__main__':
    p2 = threading.Thread(target=process_2)
p2.start()
p2.join()
