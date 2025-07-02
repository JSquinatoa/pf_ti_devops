package Controller;

import java.util.Map;

import Service.JsonArchivoService;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("/archivosjson")
public class JsonArchivoController {

    @Inject
    JsonArchivoService jsonArchivoService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> obtenerJson() {
        return jsonArchivoService.leerJsonsDesdeHDFS();
    }

    @POST
    @Path("")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public Response guardarJson(Map<String, Object> body) {
        try {
            String nombreArchivo = (String) body.get("nombre");
            Object contenido = body.get("contenido");

            if (nombreArchivo == null || contenido == null) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Faltan campos requeridos: 'nombre' y/o 'contenido'")
                        .build();
            }

            jsonArchivoService.guardarJsonEnHDFS(nombreArchivo, contenido);
            return Response.ok("Archivo guardado correctamente con nombre: " + nombreArchivo).build();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity("Error interno al guardar el JSON").build();
        }
    }

}