#include <bits/stdc++.h>
using namespace std;
int main() {
    double tradius, cradius;
    cin>>tradius>>cradius;
    double presult= tradius*tradius;
    double result = (((tradius - cradius)* (tradius-cradius))/presult);
               printf("%.6f\n", (result*100));
    return 0;
}
