package org.acm.reducemap.worker;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.python.core.Py;
import org.python.core.PyFunction;
import org.python.core.PyObject;
import org.python.core.adapter.PyObjectAdapter;
import org.python.util.PythonInterpreter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigInteger;
import java.util.Set;
import java.util.Vector;

class JyExecutor {

    private PythonInterpreter interp = new PythonInterpreter();

    JyExecutor() {
        System.setProperty("python.import.site","false");
    }

    int run(String src, String args) {
        System.out.println("JyExecutor running: args:"+args);
        JsonParser parser = new JsonParser();
        Set<String> keySet;
        JsonObject json;
        try {
            json = (JsonObject) parser.parse(args);
            keySet = json.keySet();
        } catch (JsonSyntaxException e) {
            e.printStackTrace();
            return -1;
        }
        String[] keywords = new String[keySet.size()];
        keySet.toArray(keywords);
        Vector<PyObject> pyArgs = new Vector<>();
        PyObjectAdapter adapter = new PyObjectAdapter() {
            @Override
            public boolean canAdapt(Object o) {
                return false;
            }

            @Override
            public PyObject adapt(Object o) {
                if (o instanceof Integer) return Py.newInteger((Integer) o);
                if (o instanceof BigInteger) return Py.newInteger(((BigInteger) o).longValue());
                if (o instanceof Boolean) return Py.newBoolean((Boolean) o);
                if (o instanceof Float) return Py.newFloat((Float) o);
                if (o instanceof Double) return Py.newFloat((Double) o);
                if (o instanceof String) return Py.newString(String.valueOf(o));
                return null;
            }
        };
        for (String key:keywords) {
            JsonElement obj = json.get(key);
            pyArgs.add(adapter.adapt(obj.getAsInt())); //TODO: add various types
        }
        interp.exec(src);
        PyObject[] pyArgArr = new PyObject[pyArgs.size()];
        pyArgs.toArray(pyArgArr);
        PyFunction pyFunc = interp.get("run", PyFunction.class);
        if (pyFunc == null) {
            System.out.println("function run not found");
            return -1;
        }
        PyObject res = pyFunc.__call__(pyArgArr,keywords);
        System.out.println("Jy Result:"+res);
        interp.cleanup();
//        interp.close();
        return res.asInt();
    }

    public static void main(String[] args) {
        System.setProperty("python.import.site","false");
//        System.setProperty("python.home","");
        JyExecutor executor = new JyExecutor();
        String path = "./1.py";
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();
        try {
            br = new BufferedReader(new FileReader(path));
            String t;
            while((t=br.readLine())!=null){
                sb.append(t).append('\n');
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        String arg = "{\"a\":5000, \"b\":5001}";
        executor.run(sb.toString(),arg);
    }

}
