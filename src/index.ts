import { faker } from "@faker-js/faker";
import ObjectsToCsv from "objects-to-csv";

const data: Array<object> = [];

for (let index = 0; index < 100; index++) {
  data.push({
    name: faker.name.findName(),
    age: faker.datatype.number({ min: 18, max: 70 }).toString(),
    address: faker.address.streetAddress(),
    department: faker.commerce.department(),
    email: faker.internet.email(),
    username: faker.internet.userName(),
    password: faker.internet.password(),
  });
}

new ObjectsToCsv(data).toDisk("output.csv");
