package Service;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class JsonArchivoService {

    @ConfigProperty(name = "hdfs.url")
    String hdfsUrl;

    @ConfigProperty(name = "hdfs.base.path")
    String hdfsBasePath;

    public Map<String, Object> leerJsonsDesdeHDFS() {
        Map<String, Object> jsonMap = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUrl);
            FileSystem fs = FileSystem.get(conf);

            Path path = new Path(hdfsBasePath);
            FileStatus[] archivos = fs.listStatus(path);

            for (FileStatus archivo : archivos) {
                if (archivo.isFile() && archivo.getPath().getName().endsWith(".json")) {
                    // 1. Leer el archivo
                    FSDataInputStream inputStream = fs.open(archivo.getPath());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                    StringBuilder contenido = new StringBuilder();
                    String linea;
                    while ((linea = reader.readLine()) != null) {
                        contenido.append(linea);
                    }
                    reader.close();
                    inputStream.close();

                    // 2. Obtener nombre del archivo sin extensión
                    String nombreArchivo = archivo.getPath().getName().replace(".json", "");

                    // 3. Parsear contenido a lista de objetos (puede ser Map o List)
                    Object jsonData = mapper.readValue(contenido.toString(), new TypeReference<Object>() {});

                    // 4. Guardar en el mapa
                    jsonMap.put(nombreArchivo, jsonData);
                }
            }

            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Error al leer los archivos JSON desde HDFS", e);
        }

        return jsonMap;
    }
}
