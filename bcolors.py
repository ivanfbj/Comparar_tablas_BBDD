# Esta clase permite añadir colores a los mensajes que se imprimen en la consola
class bcolors:
    OK = '\033[92m' #GREEN
    WARNING = '\033[93m' #YELLOW
    FAIL = '\033[91m' #RED
    HIGHLIGHTER = '\033[97m\033[102m' # letter WHITE -  background GREEN
    RESET = '\033[0m' #RESET COLOR
    BOLD_RED = '\033[1m\033[91m' # BOLD - letter RED
    # 0m -> reset
    # 90m - 97m -> color
    # 100m - 107m -> background color