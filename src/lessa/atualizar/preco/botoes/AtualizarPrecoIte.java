package lessa.atualizar.preco.botoes;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AtualizarPrecoIte implements AcaoRotinaJava {
    @Override
    public void doAction(ContextoAcao contextoAcao) throws Exception {
        Registro[] linhas = contextoAcao.getLinhas();

        for (Registro linha : linhas) {
            Object nunota = linha.getCampo("NUNOTA");

            JapeSession.SessionHandle hnd = null;
            JdbcWrapper jdbc = null;
            try {
                hnd = JapeSession.open();
                jdbc = EntityFacadeFactory.getDWFFacade().getJdbcWrapper();
                jdbc.openSession();

                NativeSql queryUpd = new NativeSql(jdbc);
                queryUpd.appendSql("UPDATE\n" +
                        "              \tITE1\n" +
                        "                SET VLRUNIT = (\n" +
                        "                    SELECT SANKHYA.SNK_PRECO(\n" +
                        "                        (SELECT ISNULL(CODTAB, 0)\n" +
                        "                         FROM TGFPAR\n" +
                        "                         WHERE CODPARC = (SELECT CODPARC FROM TGFCAB WHERE NUNOTA = ITE1.NUNOTA)),\n" +
                        "                         ITE1.CODPROD\n" +
                        "                    )\n" +
                        "                ),\n" +
                        "                VLRTOT = (\n" +
                        "                    SELECT SANKHYA.SNK_PRECO(\n" +
                        "                        (SELECT ISNULL(CODTAB, 0)\n" +
                        "                         FROM TGFPAR\n" +
                        "                         WHERE CODPARC = (SELECT CODPARC FROM TGFCAB WHERE NUNOTA = ITE1.NUNOTA)),\n" +
                        "                         ITE1.CODPROD\n" +
                        "                    )\n" +
                        "                ) * QTDNEG\n" +
                        "                FROM TGFITE ITE1\n" +
                        "                WHERE NUNOTA = :NUNOTA");
                queryUpd.setReuseStatements(true);
                queryUpd.setBatchUpdateSize(500);
                queryUpd.setNamedParameter("NUNOTA", nunota);
                queryUpd.addBatch();
                queryUpd.cleanParameters();
                queryUpd.flushBatchTail();
                NativeSql.releaseResources(queryUpd);

            } catch (Exception e) {
                MGEModelException.throwMe(e);
            } finally {
                JdbcWrapper.closeSession(jdbc);
                JapeSession.close(hnd);
            }
        }
    }
}
