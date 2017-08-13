public class Problem_A {
    public static void main(String[] args) {
        Scanner s = new Scanner(System.in);
        for (int numOfTestCases = 0; numOfTestCases < 100; numOfTestCases++) {
        int numberOfPersons;
        double startXCoordinate = 0, startYCoordinate = 0, destinationX = 0, 
                destinationY = 0, initAngle = 0, walkDist = 0, worstDist = Double.MAX_VALUE, 
                dxSum = 0, dySum = 0, temp = 0;
        double[] destXCoordinate = new double[20];
        double[] destYCoordinate = new double[20];
        numberOfPersons = s.nextInt();
        for (int iterVbl = 0; iterVbl < numberOfPersons; iterVbl++) {
            initAngle = 0;
            walkDist = 0;
            startXCoordinate = s.nextDouble();
            startYCoordinate = s.nextDouble();
            String rule;
            rule = s.nextLine();
            String[] splitStr = rule.split(" ");
            if (splitStr[1].equals("start")) {
                initAngle = Double.parseDouble(splitStr[2]);
                walkDist = Double.parseDouble(splitStr[4]);
                destinationX = startXCoordinate + (walkDist * Math.cos(Math.toRadians(initAngle)));
                destinationY = startYCoordinate + (walkDist * Math.sin(Math.toRadians(initAngle)));
            }
            for (int index = 5; index < splitStr.length;) {
                if (splitStr[index].equals("turn")) {
                    initAngle = initAngle + Double.parseDouble(splitStr[index + 1]);
                    index = index + 2;
                }
                if (splitStr[index].equals("walk")) {
                    walkDist = Double.parseDouble(splitStr[index + 1]);
                    index = index + 2;
                }
                destinationX = destinationX + (walkDist * Math.cos(Math.toRadians(initAngle)));
                destinationY = destinationY + (walkDist * Math.sin(Math.toRadians(initAngle)));
            }
            destXCoordinate[iterVbl] = destinationX;
            destYCoordinate[iterVbl] = destinationY;
        }
        for (int iterVbl = 0; iterVbl < numberOfPersons; iterVbl++) {
            dxSum += destXCoordinate[iterVbl];
            dySum += destYCoordinate[iterVbl];
        }
        
        destinationX= dxSum / numberOfPersons;
        destinationY= dySum / numberOfPersons;

        for (int iterVbl = 0; iterVbl < numberOfPersons; iterVbl++) {
            temp = Math.sqrt((destinationX - destXCoordinate[iterVbl]) * (destinationX - destXCoordinate[iterVbl]) + (destinationY - destYCoordinate[iterVbl]) * (destinationY - destYCoordinate[iterVbl]));
            if (temp < worstDist) {
                worstDist = temp;
            }
        }
        System.out.println("" + destinationX + " " + destinationY + " " + worstDist);

    }
    }
}