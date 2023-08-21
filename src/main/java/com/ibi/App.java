package com.ibi;

import java.util.Scanner;

public class App {
    static Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        AppMethods.pgConnect();

        if (AppMethods.isUpdateRequired()) {
            AppMethods.updateRates();
        }

        while (true) {
            System.out.print("Convert: "); // FORMAT: "Value Currency" e.g. "100 USD"
            double value = scanner.nextDouble();
            String from = scanner.nextLine().trim();
            System.out.print("to: "); // FORMAT: "Currency" e.g. "GBP"
            String to = scanner.nextLine();
            System.out.println(AppMethods.convert(value, from, to));
            System.out.println("Continue?"); // FORMAT: "Y" OR "N"
            if (scanner.nextLine().charAt(0) == 'N') {
                break;
            }
        }
        scanner.close();
        AppMethods.pgDisconnect();
    }
}
