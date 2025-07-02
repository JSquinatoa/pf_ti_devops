from clienthadoop import enviar_json

# Como se deben enviar los datos

# nombre es el nombre del archivo que se le va a dar al Json en este caso 
# seria el nombre del grupo o perosona que va a medir el modelo
nombre = "Grupo_55"
# contenido es el formato que debe tener el json del segundo 1 al 30 apra que se consuma correctamente desde la vista
contenido = [
    {"segundo": 1, "atencion": 0.98},
    {"segundo": 2, "atencion": 0.97}
]

# método donde se envia el nombre y el contenido
enviar_json(nombre, contenido)