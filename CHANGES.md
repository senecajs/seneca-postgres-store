## 2.0.0: 2016-04-05

Removed the internal conversion that the plugin was making from CamelCase column names to Snake case. Backward compatibility is ensured by allowing the user to pass into options two functions named toColumnName() and fromColumnName() that make this conversion. These should implement the CamelCase to Snake case conversion. More details are provided in the [seneca-standard-query](https://github.com/senecajs/seneca-standard-query) **Column name transformation, backward compatibility section**.

All query generation code related to basic seneca functionality was moved to [seneca-standard-query](https://github.com/senecajs/seneca-standard-query) and the extended query functionality moved to [seneca-store-query](https://github.com/senecajs/seneca-store-query). This doesn't change functionality but enables functionality reuse into other stores.
