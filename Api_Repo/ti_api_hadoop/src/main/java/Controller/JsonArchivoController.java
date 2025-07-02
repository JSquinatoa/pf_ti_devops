package Controller;

import java.util.List;
import java.util.Map;

import Service.JsonArchivoService;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("/archivosjson")
public class JsonArchivoController {

    @Inject
    JsonArchivoService jsonArchivoService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Map<String, Object> obtenerJson() {
        return jsonArchivoService.leerJsonsDesdeHDFS();
    }

}