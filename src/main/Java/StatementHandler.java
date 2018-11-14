import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;

import java.util.HashSet;
import java.util.Set;

public class StatementHandler implements RDFHandler {
    private Set<Statement> statements;
    public StatementHandler() {
        statements = new HashSet<>();
    }

    @Override
    public void startRDF() throws RDFHandlerException {

    }

    @Override
    public void endRDF() throws RDFHandlerException {

    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {

    }

    @Override
    public void handleStatement(Statement st) throws RDFHandlerException {
        statements.add(st);
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {

    }
}
