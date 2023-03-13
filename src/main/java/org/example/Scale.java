package org.example;

import org.json.JSONObject;

import java.sql.*;
import java.util.*;


public class Scale {
    static Scale scale;

    public static void main(String[] args) {
        Locale.setDefault(Locale.ENGLISH);
        scale = new Scale();

        try (Connection con = dbMysql.startConnection();
             Statement stmt = con.createStatement();
             Statement stUpdate = con.createStatement();
             Connection con2 = dbSql.startConnection();
             Statement stInsert = con2.createStatement()) {
            ResultSet rs = dbMysql.getSelect(stmt,
                    "select IdEmpresa, Tipo as TipoTicket, IdBalanzaMaestra, NumTicket, IdSeccion, IdVendedor, " +
                            "IdCliente, NumLineas,ImporteTotal, Fecha as FechaInicio, Fecha as FechaFin, " +
                            "0 as StatusTraspasado, idticket from sys_datos.dat_ticket_cabecera where Enviado = 0");

            while (rs.next()) {
                System.out.println("IDTICKET: " + scale.SelectCabecera(rs, stUpdate, stInsert));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

    }

    private String SelectCabecera(ResultSet rs, Statement stupdate, Statement stinsert) throws SQLException {

        JSONObject cabecera = new JSONObject();
        //comprobar si idempresa es null, si es null poner 0
        String idEmpresa = rs.getString("IdEmpresa");
        cabecera.put("IdEmpresa", Objects.requireNonNullElse(idEmpresa, 0));

        String tipoTicket = rs.getString("TipoTicket");
        cabecera.put("TipoTicket", Objects.requireNonNullElse(tipoTicket, ""));

        String idBalanzaMaestra = rs.getString("IdBalanzaMaestra");
        cabecera.put("IdBalanzaMaestra", Objects.requireNonNullElse(idBalanzaMaestra, 0));

        String numTicket = rs.getString("NumTicket");
        cabecera.put("NumTicket", Objects.requireNonNullElse(numTicket, 0));

        String idSeccion = rs.getString("IdSeccion");
        cabecera.put("IdSeccion", Objects.requireNonNullElse(idSeccion, 0));

        String idVendedor = rs.getString("IdVendedor");
        cabecera.put("IdVendedor", Objects.requireNonNullElse(idVendedor, 0));

        String idCliente = rs.getString("IdCliente");
        cabecera.put("IdCliente", Objects.requireNonNullElse(idCliente, 0));

        String numLineas = rs.getString("NumLineas");
        cabecera.put("NumLineas", Objects.requireNonNullElse(numLineas, 0));

        String importeTotal = rs.getString("ImporteTotal");
        cabecera.put("ImporteTotal", Objects.requireNonNullElse(importeTotal, 0));

        String fechaInicio = rs.getString("FechaInicio");
        cabecera.put("FechaInicio", Objects.requireNonNullElse(fechaInicio, ""));

        String fechaFin = rs.getString("FechaFin");
        cabecera.put("FechaFin", Objects.requireNonNullElse(fechaFin, ""));

        cabecera.put("bTIC_IdTicket", UUID.randomUUID().toString());

        String id = rs.getString("idticket");


        String insert = String.format("insert into [MBVic].[dbo].[bTIC_CabeceraTicket4] values " +
                        "(%s,'%s', %s, %s, %s, %s, %s, %s, %s, '%s', '%s', 0, 0, '%s');",
                cabecera.getInt("IdEmpresa"),
                cabecera.getString("TipoTicket"),
                cabecera.getInt("IdBalanzaMaestra"),
                cabecera.getInt("NumTicket"),
                cabecera.getInt("IdSeccion"),
                cabecera.getInt("IdVendedor"),
                cabecera.getInt("IdCliente"),
                cabecera.getInt("NumLineas"),
                String.format("%.0f", Double.parseDouble(cabecera.getString("ImporteTotal")) * 100),
                cabecera.getString("FechaInicio"),
                cabecera.getString("FechaFin"),
                cabecera.getString("bTIC_IdTicket"));

        System.out.println(insert);
        dbSql.insert(stinsert, insert);

        dbMysql.update(stupdate, "update sys_datos.dat_ticket_cabecera set Enviado = 1 where idticket = " + id);
        String idTicket = cabecera.getString("bTIC_IdTicket");
        scale.SelectLineas(idTicket, id, idEmpresa);
        scale.SelectFormaPago(idTicket, id, idEmpresa);
        return idTicket;
    }

    private void SelectFormaPago(String idTicket, String idMysql, String idEmpresa) {
        try (Connection con = dbMysql.startConnection();
             Statement stmt = con.createStatement();
             Connection con2 = dbSql.startConnection();
             Statement stmt2 = con2.createStatement()) {
            ResultSet rs = dbMysql.getSelect(stmt, String.format("""
                    SELECT
                        d.`IdEmpresa`,
                        d.`IdBalanzaMaestra`,
                        d.`NumTicket`,
                        d.`IdTienda`,
                        c.`IdVendedor`,
                        d.`IdFormaPago`,
                        d.`ImporteFormaPago`,
                        d.`Contado`,
                        d.`TimeStamp`,
                        d.`TimeStamp` AS TimeStamp2
                    FROM
                        dat_ticket_forma_pago d
                        join dat_ticket_cabecera c on c.idticket = d.idticket
                        and c.idempresa = d.idempresa
                    where
                        c.`Enviado` = 1
                        and c.`IdEmpresa` = %s
                        and c.`idTicket` = %s
                    """, idEmpresa, idMysql));
            while (rs.next()) {
                JSONObject formaPago = new JSONObject();
                formaPago.put("IdEmpresa", Objects.requireNonNullElse(idEmpresa, 0));

                String idBalanzaMaestra = rs.getString("IdBalanzaMaestra");
                formaPago.put("IdBalanzaMaestra", Objects.requireNonNullElse(idBalanzaMaestra, 0));

                String numTicket = rs.getString("NumTicket");
                formaPago.put("NumTicket", Objects.requireNonNullElse(numTicket, 0));

                String idTienda = rs.getString("IdTienda");
                formaPago.put("IdTienda", Objects.requireNonNullElse(idTienda, 0));

                String idVendedor = rs.getString("IdVendedor");
                formaPago.put("IdVendedor", Objects.requireNonNullElse(idVendedor, 0));

                String idFormaPago = rs.getString("IdFormaPago");
                formaPago.put("IdFormaPago", Objects.requireNonNullElse(idFormaPago, 0));

                String importeFormaPago = rs.getString("ImporteFormaPago");
                formaPago.put("ImporteFormaPago", Objects.requireNonNullElse(importeFormaPago, 0));

                String contado = rs.getString("Contado");
                formaPago.put("Contado", Objects.requireNonNullElse(contado, 0));

                String timeStamp = rs.getString("TimeStamp");
                formaPago.put("TimeStamp", Objects.requireNonNullElse(timeStamp, ""));

                String timeStamp2 = rs.getString("TimeStamp2");
                formaPago.put("TimeStamp2", Objects.requireNonNullElse(timeStamp2, ""));

                String insert = String.format("insert into [MBVic].[dbo].[bTIC_FormaPagTicket4] " +
                                "values (%s, %s, %s, %s, %s, %s, %s, %s, '%s', '%s', NEWID(), '%s');",
                        formaPago.getString("IdEmpresa"),
                        formaPago.getString("IdBalanzaMaestra"),
                        formaPago.getString("NumTicket"),
                        formaPago.getString("IdTienda"),
                        formaPago.getString("IdVendedor"),
                        formaPago.getString("IdFormaPago"),
                        formaPago.getString("ImporteFormaPago"),
                        formaPago.getString("Contado"),
                        formaPago.getString("TimeStamp"),
                        formaPago.getString("TimeStamp2"),
                        idTicket);
                System.out.println(insert);

                dbSql.insert(stmt2, insert);
            }
        } catch (Exception e) {
            System.err.println("SELECT PAGO: " + e.getMessage());
        }
    }

    private void SelectLineas(String idTicket, String idMysql, String idEmpresa) {
        try (Connection con = dbMysql.startConnection();
             Statement stmt = con.createStatement();
             Connection con2 = dbSql.startConnection();
             Statement stmt2 = con2.createStatement()) {
            ResultSet rs = dbMysql.getSelect(stmt, String.format("""
                    SELECT
                        d.`IdEmpresa`,
                        d.`IdBalanzaMaestra`,
                        c.`NumTicket`,
                        d.`IdDepartamento`,
                        c.`IdVendedor`,
                        d.`IdArticulo`,
                        d.`IdFamilia`,
                        d.`Cantidad2`,
                        d.`Peso`,
                        d.`Precio`,
                        d.`Importe`,
                        d.`RecargoEquivalencia`,
                        d.`IdIVA`,
                        d.`TextoLote`,
                        d.`EstadoLinea`,
                        d.`TimeStamp`,
                        d.`TimeStamp` as TimeStamp2,
                        d.Descuento,
                        d.`Descuento` as Descuento2,
                        d.`Descripcion`,
                        a.`IdTipo`
                    FROM
                        dat_ticket_linea d
                        join dat_ticket_cabecera c on c.idticket = d.idticket
                        and c.idempresa = d.idempresa
                        join dat_articulo a on d.idarticulo = a.idarticulo
                    where
                        c.`Enviado` = 1 and c.`IdEmpresa` = %s and c.`idTicket` = %s""", idEmpresa, idMysql));
            while (rs.next()) {
                JSONObject linea = new JSONObject();

                linea.put("IdEmpresa", Objects.requireNonNullElse(idEmpresa, 0));

                String idBalanzaMaestra = rs.getString("IdBalanzaMaestra");
                linea.put("IdBalanzaMaestra", Objects.requireNonNullElse(idBalanzaMaestra, 0));

                String numTicket = rs.getString("NumTicket");
                linea.put("NumTicket", Objects.requireNonNullElse(numTicket, 0));

                String idDepartamento = rs.getString("IdDepartamento");
                linea.put("IdDepartamento", Objects.requireNonNullElse(idDepartamento, 0));

                String vendedor = rs.getString("IdVendedor");
                linea.put("Vendedor", Objects.requireNonNullElse(vendedor, 0));

                String idArticulo = rs.getString("IdArticulo");
                linea.put("IdArticulo", Objects.requireNonNullElse(idArticulo, 0));

                String idFamilia = rs.getString("IdFamilia");
                linea.put("IdFamilia", Objects.requireNonNullElse(idFamilia, 0));

                String cantidad2 = rs.getString("Peso");
                linea.put("Cantidad2", Objects.requireNonNullElse(cantidad2, 0));

                if (rs.getString("IdTipo").equals("2")) {
                    String peso = "0";
                    linea.put("Peso", peso);
                } else {
                    String peso = rs.getString("Peso");
                    linea.put("Peso", Objects.requireNonNullElse(peso, 0));
                }

                String precio = rs.getString("Precio");
                linea.put("Precio", Objects.requireNonNullElse(precio, 0));

                String importe = rs.getString("Importe");
                linea.put("Importe", Objects.requireNonNullElse(importe, 0));

                String recargoEquivalencia = rs.getString("RecargoEquivalencia");
                linea.put("RecargoEquivalencia", Objects.requireNonNullElse(recargoEquivalencia, 0));

                String idIVA = rs.getString("IdIVA");
                linea.put("IdIVA", Objects.requireNonNullElse(idIVA, 0));

                String textoLote = rs.getString("TextoLote");
                linea.put("TextoLote", Objects.requireNonNullElse(textoLote, ""));

                String estadoLinea = rs.getString("EstadoLinea");
                linea.put("EstadoLinea", Objects.requireNonNullElse(estadoLinea, 0));

                String timeStamp = rs.getString("TimeStamp");
                linea.put("TimeStamp", Objects.requireNonNullElse(timeStamp, ""));

                String timeStamp2 = rs.getString("TimeStamp2");
                linea.put("TimeStamp2", Objects.requireNonNullElse(timeStamp2, ""));

                String descuento = rs.getString("Descuento");
                linea.put("Descuento", Objects.requireNonNullElse(descuento, 0));

                String descuento2 = rs.getString("Descuento2");
                linea.put("Descuento2", Objects.requireNonNullElse(descuento2, 0));

                String descripcion = rs.getString("Descripcion");
                linea.put("Descripcion", Objects.requireNonNullElse(descripcion, ""));

                linea.put("LineasPosicion", UUID.randomUUID().toString());
                if (linea.getString("LineasPosicion") == null) {
                    linea.put("LineasPosicion", "");
                }

                String insert = String.format("insert into [MBVic].[dbo].[bTIC_LineasTicket4] " +
                                "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, '%s', %s, '%s', '%s', %s, %s, '%s', '%s', '%s')",
                        linea.getInt("IdEmpresa"),
                        linea.getInt("IdBalanzaMaestra"),
                        linea.getInt("NumTicket"),
                        linea.getInt("IdDepartamento"),
                        linea.getInt("Vendedor"),
                        linea.getInt("IdArticulo"),
                        linea.getInt("IdFamilia"),
                        linea.getString("Cantidad2"),
                        String.format("%s",linea.getDouble("Peso") * 1000),
                        linea.getDouble("Precio"),
                        linea.getDouble("Importe"),
                        linea.getDouble("RecargoEquivalencia"),
                        linea.getInt("IdIVA"),
                        linea.getString("TextoLote"),
                        linea.getString("EstadoLinea"),
                        linea.getString("TimeStamp"),
                        linea.getString("TimeStamp2"),
                        String.format("%.2f", linea.getDouble("Descuento") * linea.getDouble("Precio")),
                        String.format("%.2f", linea.getDouble("Descuento2")),
                        linea.getString("Descripcion"),
                        linea.getString("LineasPosicion"),
                        idTicket);
                System.out.println(insert);
                dbSql.insert(stmt2, insert);
            }
        } catch (Exception e) {
            System.err.println("SELECT LINEAS: " + e.getMessage());
        }
    }
}