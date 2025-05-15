package reaktime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package reaktime.bean.DimCategoryCompare
 * @Author chen.ming
 * @Date 2025/5/14 22:23
 * @description: DimCategoryCompare
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
