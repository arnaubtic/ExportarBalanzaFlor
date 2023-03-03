package org.example;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Scale {
    List<String> inserts = new ArrayList<>();
    String user = "user";
    String password = "dibal";

    public static void main(String[] args) {
        Scale scale = new Scale();
        scale.SelectCabecera();
        scale.SelectFormaPago();
        scale.SelectLineas();
        scale.inserts.forEach(System.out::println);
        /*
        if (scale.inserts.size() == 0) {
            System.out.println("No inserts to execute");
            return;
        }
        if (scale.ExecuteInserts() == 0) {
            System.out.println("Inserts executed successfully");
            scale.StatusEnviado();
        } else {
            System.out.println("Error executing inserts");
        }
         */
    }

    private void StatusEnviado() {
        try {
            Connection con = DriverManager.getConnection("jdbc:mysql://10.10.139.191:3306/", user, password);
            Statement stmt = con.createStatement();
            stmt.executeUpdate("update sys_datos.dat_ticket_cabecera set Enviado = 1 where Enviado = 0");
            System.out.println("Status Enviado updated successfully");
            con.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private int ExecuteInserts() {
        try (Connection con = dbSql.startConnection();
             Statement stmt = con.createStatement()) {
            for (String insert : inserts) {
                stmt.executeUpdate(insert);
            }
            return 0;
        } catch (Exception e) {
            System.err.println("EXECUTE INSERT: " + e.getMessage());
        }
        return -1;
    }

    private void SelectCabecera() {
        try (Connection con = dbMysql.startConnection();
             Statement stmt = con.createStatement()) {
            //print columns
            /*
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            for (int i = 1; i <= columnsNumber; i++) {
                if (i > 1) System.out.print("\t");
                String columnValue = rsmd.getColumnName(i);
                System.out.print(columnValue);
            }
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print("\t");
                    String columnValue = rs.getString(i);
                    System.out.print(columnValue);
                }
                System.out.println();
            }
             */
            ResultSet rs = dbMysql.getSelect(stmt, "select IdEmpresa, Tipo as TipoTicket, IdBalanzaMaestra, NumTicket, IdSeccion, IdVendedor, " +
                    "IdCliente, NumLineas,ImporteTotal, Fecha as FechaInicio, Fecha as FechaFin, 0 as StatusTraspasado from sys_datos.dat_ticket_cabecera where Enviado = 0");
            while (rs.next()) {
                String IdEmpresa = rs.getString("IdEmpresa");
                if (IdEmpresa == null) IdEmpresa = "0";
                String TipoTicket = rs.getString("TipoTicket");
                if (TipoTicket == null) TipoTicket = " ";
                String IdBalanzaMaestra = rs.getString("IdBalanzaMaestra");
                if (IdBalanzaMaestra == null) IdBalanzaMaestra = "0";
                String NumTicket = rs.getString("NumTicket");
                if (NumTicket == null) NumTicket = "0";
                String IdSeccion = rs.getString("IdSeccion");
                if (IdSeccion == null) IdSeccion = "0";
                String IdVendedor = rs.getString("IdVendedor");
                if (IdVendedor == null) IdVendedor = "0";
                String IdCliente = rs.getString("IdCliente");
                if (IdCliente == null) IdCliente = "0";
                String NumLineas = rs.getString("NumLineas");
                if (NumLineas == null) NumLineas = "0";
                String ImporteTotal = rs.getString("ImporteTotal");
                if (ImporteTotal == null) ImporteTotal = "0";
                String FechaInicio = rs.getString("FechaInicio");
                if (FechaInicio == null) FechaInicio = "0";
                String FechaFin = rs.getString("FechaFin");
                if (FechaFin == null) FechaFin = "0";
                String StatusTraspasado = rs.getString("StatusTraspasado");
                if (StatusTraspasado == null) StatusTraspasado = "0";
                inserts.add(String.format("insert into [localdb].[dbo].[bTIC_CabecerTickets4]" +
                                "values (%s,'%s', %s, %s, %s, %s, %s, %s, %s, '%s', '%s', %s);",
                        IdEmpresa, TipoTicket, IdBalanzaMaestra, NumTicket, IdSeccion, IdVendedor, IdCliente, NumLineas,
                        String.format("%.0f", Double.parseDouble(ImporteTotal) * 100), FechaInicio, FechaFin, StatusTraspasado));
            }
        } catch (Exception e) {
            System.err.println("SELECT: " + e.getMessage());
        }
    }

    private void SelectFormaPago() {
        try (Connection con = dbMysql.startConnection();
             Statement stmt = con.createStatement()) {
            ResultSet rs = dbMysql.getSelect(stmt, "SELECT d.`IdEmpresa`, d.`IdBalanzaMaestra`, d.`NumTicket`, d.`IdTienda`, d.`Usuario`, d.`IdFormaPago`, d.`ImporteFormaPago`, d.`Contado`, d.`TimeStamp`, d.`TimeStamp` FROM dat_ticket_forma_pago d where (select Enviado from dat_ticket_cabecera a where a.idticket = d.idticket) = 0");
            while (rs.next()) {
                String IdEmpresa = rs.getString("IdEmpresa");
                if (IdEmpresa == null) IdEmpresa = "0";
                String IdBalanzaMaestra = rs.getString("IdBalanzaMaestra");
                if (IdBalanzaMaestra == null) IdBalanzaMaestra = "0";
                String NumTicket = rs.getString("NumTicket");
                if (NumTicket == null) NumTicket = "0";
                String IdTienda = rs.getString("IdTienda");
                if (IdTienda == null) IdTienda = "0";
                String Usuario = rs.getString("Usuario");
                if (Usuario == null) Usuario = "0";
                String IdFormaPago = rs.getString("IdFormaPago");
                if (IdFormaPago == null) IdFormaPago = "0";
                String ImporteFormaPago = rs.getString("ImporteFormaPago");
                if (ImporteFormaPago == null) ImporteFormaPago = "0";
                String Contado = rs.getString("Contado");
                if (Contado == null) Contado = "0";
                String TimeStamp = rs.getString("TimeStamp");
                if (TimeStamp == null) TimeStamp = "0";
                inserts.add(String.format("insert into [localdb].[dbo].[bTIC_FormaPagTicket4]" +
                                "values (%s, %s, %s, %s, %s, %s, %s, %s, '%s');",
                        IdEmpresa, IdBalanzaMaestra, NumTicket, IdTienda, Usuario, IdFormaPago, ImporteFormaPago,
                        Contado, TimeStamp));
            }
        } catch (Exception e) {
            System.err.println("SELECT PAGO: " + e.getMessage());
        }
    }

    private void SelectLineas() {
        try (Connection con = dbMysql.startConnection();
             Statement stmt = con.createStatement()) {
            ResultSet rs = dbMysql.getSelect(stmt, "SELECT d.`IdEmpresa`, d.`IdBalanzaMaestra`, (select numticket from dat_ticket_cabecera a where a.idticket = d.idticket) as NumTicket, d.`IdDepartamento`, d.`Usuario`, d.`IdArticulo`, d.`IdFamilia`, d.`Cantidad2`, d.`Peso`, d.`Precio`, d.`Importe`, d.`RecargoEquivalencia`, d.`IdIVA`, d.`TextoLote`, d.`EstadoLinea`, d.`TimeStamp`, d.`TimeStamp` as TimeStamp2, d.Descuento, d.`Descuento` as Descuento2, d.`Descripcion` FROM dat_ticket_linea d where (select Enviado from dat_ticket_cabecera a where a.idticket = d.idticket) = 0");
            while (rs.next()) {
                String IdEmpresa = rs.getString("IdEmpresa");
                if (IdEmpresa == null) IdEmpresa = "0";
                String IdBalanzaMaestra = rs.getString("IdBalanzaMaestra");
                if (IdBalanzaMaestra == null) IdBalanzaMaestra = "0";
                String NumTicket = rs.getString("NumTicket");
                if (NumTicket == null) NumTicket = "0";
                String IdDepartamento = rs.getString("IdDepartamento");
                if (IdDepartamento == null) IdDepartamento = "0";
                String Usuario = rs.getString("Usuario");
                if (Usuario == null) Usuario = "0";
                String IdArticulo = rs.getString("IdArticulo");
                if (IdArticulo == null) IdArticulo = "0";
                String IdFamilia = rs.getString("IdFamilia");
                if (IdFamilia == null) IdFamilia = "0";
                String Cantidad2 = rs.getString("Cantidad2");
                if (Cantidad2 == null) Cantidad2 = "0";
                String Peso = rs.getString("Peso");
                if (Peso == null) Peso = "0";
                String Precio = rs.getString("Precio");
                if (Precio == null) Precio = "0";
                String Importe = rs.getString("Importe");
                if (Importe == null) Importe = "0";
                String RecargoEquivalencia = rs.getString("RecargoEquivalencia");
                if (RecargoEquivalencia == null) RecargoEquivalencia = "0";
                String IdIVA = rs.getString("IdIVA");
                if (IdIVA == null) IdIVA = "0";
                String TextoLote = rs.getString("TextoLote");
                if (TextoLote == null) TextoLote = "0";
                String EstadoLinea = rs.getString("EstadoLinea");
                if (EstadoLinea == null) EstadoLinea = "0";
                String TimeStamp = rs.getString("TimeStamp");
                if (TimeStamp == null) TimeStamp = "0";
                String TimeStamp2 = rs.getString("TimeStamp2");
                if (TimeStamp2 == null) TimeStamp2 = "0";
                String Descuento = rs.getString("Descuento");
                if (Descuento == null) Descuento = "0";
                String Descuento2 = rs.getString("Descuento2");
                if (Descuento2 == null) Descuento2 = "0";
                String Descripcion = rs.getString("Descripcion");
                if (Descripcion == null) Descripcion = "0";
                inserts.add(String.format("insert into [localdb].[dbo].[bTIC_LineasTickets4]" +
                                "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        IdEmpresa, IdBalanzaMaestra, NumTicket, IdDepartamento, Usuario, IdArticulo, IdFamilia,
                        Cantidad2, Peso, Precio, Importe, RecargoEquivalencia, IdIVA, TextoLote, EstadoLinea, TimeStamp,
                        TimeStamp2, Descuento, Descuento2, Descripcion));
            }
        } catch (Exception e) {
            System.err.println("SELECT LINEAS: " + e.getMessage());
        }
    }
}